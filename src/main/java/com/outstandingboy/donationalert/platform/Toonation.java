package com.outstandingboy.donationalert.platform;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.outstandingboy.donationalert.entity.Donation;
import com.outstandingboy.donationalert.exception.TokenNotFoundException;

import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;

public class Toonation implements Platform {
    private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";
    private static final String WS_BASE_URL = "wss://ws.toon.at";

    private String payload;
    private Subject<Donation> donationObservable;
    private Subject<String> messageObservable;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final OkHttpClient pageClient;
    private final OkHttpClient socketClient;
    private final java.util.concurrent.atomic.AtomicReference<WebSocket> socketRef = new java.util.concurrent.atomic.AtomicReference<>();
    private final WebSocketListener listener;
    private final ScheduledExecutorService scheduler;
    private final CountDownLatch openLatch = new CountDownLatch(1);
    private final AtomicInteger openCount = new AtomicInteger(0);

    public Toonation(String key) throws IOException {
        pageClient = new OkHttpClient.Builder()
            .callTimeout(20, TimeUnit.SECONDS)
            .build();

        socketClient = new OkHttpClient.Builder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .build();

        Request pageRequest = new Request.Builder()
            .url("https://toon.at/widget/alertbox/" + key)
            .header("User-Agent", USER_AGENT)
            .header("Accept-Language", "ko-KR,ko;q=0.9,en;q=0.8")
            .build();

        String html;
        try (Response pageResponse = pageClient.newCall(pageRequest).execute()) {
            ResponseBody body = pageResponse.body();
            if (!pageResponse.isSuccessful() || body == null) {
                throw new IOException("투네이션 위젯 페이지를 불러오지 못했습니다. status=" + pageResponse.code());
            }
            html = body.string();
        }

        Document doc = Jsoup.parse(html);
        Elements scriptElements = doc.getElementsByTag("script");
        String script = scriptElements.stream()
            .filter(e -> !e.hasAttr("src"))
            .map(Element::html)
            .collect(Collectors.joining("\n"));

        String parsedPayload = parsePayload(script);

        if (parsedPayload == null) {
            throw new TokenNotFoundException("투네이션 페이로드를 찾을 수 없습니다.");
        }

        this.payload = parsedPayload;

        donationObservable = PublishSubject.<Donation>create().toSerialized();
        // Connection messages may occur before subscribeMessage() is called.
        // Replay a small buffer so early lifecycle messages aren't lost.
        messageObservable = ReplaySubject.<String>createWithSize(16).toSerialized();

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Toonation-scheduler");
            t.setDaemon(true);
            return t;
        });

        Request request = new Request.Builder()
                .url(buildWsUrl(payload))
                .header("User-Agent", USER_AGENT)
                .build();

        this.listener = new InternalListener(socketClient, request, socketRef, closed, donationObservable, messageObservable, scheduler, openLatch, openCount);
        socketRef.set(socketClient.newWebSocket(request, listener));
    }

    private static String buildWsUrl(String payload) {
        String p = payload == null ? "" : payload.trim();
        if (p.startsWith("/")) {
            p = p.substring(1);
        }
        return WS_BASE_URL + "/" + p;
    }

    /**
     * Waits until the websocket connection is opened at least once.
     * Useful for verifying connectivity right after creating the instance.
     */
    public boolean awaitConnected(long timeout, TimeUnit unit) throws InterruptedException {
        if (openCount.get() > 0) {
            return true;
        }
        return openLatch.await(timeout, unit);
    }

    private String parsePayload(String script) {
        Pattern[] patterns = new Pattern[] {
                // Legacy: "payload":"..."
                Pattern.compile("\\\"payload\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"", Pattern.DOTALL),

                // Current: payload\u0022:\u0022...\u0022 (quotes are unicode-escaped inside a JSON.parse string)
                Pattern.compile("payload\\\\u0022\\s*:\\s*\\\\u0022([A-Za-z0-9._=-]+)\\\\u0022", Pattern.DOTALL),

                // Escaped-quotes variant: \"payload\":\"...\"
                Pattern.compile("\\\\\\\"payload\\\\\\\"\\s*:\\s*\\\\\\\"([A-Za-z0-9._=-]+)\\\\\\\"", Pattern.DOTALL)
        };

        for (Pattern pattern : patterns) {
            Matcher m = pattern.matcher(script);
            if (m.find()) {
                return m.group(1);
            }
        }

        return null;
    }

    private static Donation getDonation(JSONObject json) {
        try {
            if (json == null) return null;

            Object contentObj = json.get("content");
            if (contentObj instanceof JSONObject) {
                json = (JSONObject) contentObj;
            }

            String id = firstNonBlank(
                asString(json.get("account")),
                asString(json.get("watcher_id")),
                asString(json.get("userId")),
                asString(json.get("id"))
            );

            String nickname = firstNonBlank(
                asString(json.get("name")),
                asString(json.get("nickname")),
                asString(json.get("nickName"))
            );

            Long amount = firstNonNull(
                asLong(json.get("amount")),
                asLong(json.get("payAmount"))
            );

            String comment = firstNonNull(
                asString(json.get("message")),
                asString(json.get("comment")),
                asString(json.get("msg"))
            );

            // Heuristic: treat as donation only when key fields exist
            if (nickname == null || amount == null) {
                return null;
            }

            Donation donation = new Donation();
            donation.setId(id);
            donation.setNickName(nickname);
            donation.setAmount(amount);
            donation.setComment(comment);
            return donation;
        }
        catch (Exception e){
            return null;
        }
    }

    private static String asString(Object value) {
        if (value == null) return null;
        String s = String.valueOf(value);
        return s.isEmpty() ? null : s;
    }

    private static Long asLong(Object value) {
        if (value == null) return null;
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.isEmpty()) return null;
            try {
                return Long.valueOf(s);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... values) {
        if (values == null) return null;
        for (T v : values) {
            if (v != null) return v;
        }
        return null;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String v : values) {
            if (v != null && !v.trim().isEmpty()) {
                return v;
            }
        }
        return null;
    }

    @Override
    public Subject<Donation> getDonationObservable() {
        return donationObservable;
    }

    @Override
    public Subject<String> getMessageObservable() {
        return messageObservable;
    }

    @Override
    public Disposable subscribeDonation(Consumer<Donation> onNext) {
        return donationObservable.subscribe(onNext);
    }

    @Override
    public Disposable subscribeMessage(Consumer<String> onNext) {
        return messageObservable.subscribe(onNext);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        donationObservable.onComplete();
        messageObservable.onComplete();

        WebSocket socket = socketRef.getAndSet(null);
        if (socket != null) {
            socket.close(1000, null);
        }

        scheduler.shutdownNow();

        pageClient.dispatcher().executorService().shutdown();
        pageClient.connectionPool().evictAll();

        socketClient.dispatcher().executorService().shutdown();
        socketClient.connectionPool().evictAll();
    }

    private static final class InternalListener extends WebSocketListener {
        private final OkHttpClient socketClient;
        private final Request request;
        private final java.util.concurrent.atomic.AtomicReference<WebSocket> socketRef;
        private final AtomicBoolean closed;
        private final Subject<Donation> donationObservable;
        private final Subject<String> messageObservable;
        private final ScheduledExecutorService scheduler;
        private final CountDownLatch openLatch;
        private final AtomicInteger openCount;
        private volatile boolean timeout;
        private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
        private volatile ScheduledFuture<?> pingTask;

        private InternalListener(
            OkHttpClient socketClient,
            Request request,
            java.util.concurrent.atomic.AtomicReference<WebSocket> socketRef,
            AtomicBoolean closed,
            Subject<Donation> donationObservable,
            Subject<String> messageObservable,
            ScheduledExecutorService scheduler,
            CountDownLatch openLatch,
            AtomicInteger openCount
        ) {
            this.socketClient = socketClient;
            this.request = request;
            this.socketRef = socketRef;
            this.closed = closed;
            this.donationObservable = donationObservable;
            this.messageObservable = messageObservable;
            this.scheduler = scheduler;
            this.openLatch = openLatch;
            this.openCount = openCount;
        }

        @Override
        public void onOpen(WebSocket webSocket, Response response) {
            openCount.incrementAndGet();
            openLatch.countDown();
            reconnectAttempts.set(0);

            ScheduledFuture<?> prev = pingTask;
            if (prev != null) {
                prev.cancel(true);
            }
            pingTask = scheduler.scheduleAtFixedRate(() -> {
                try {
                    if (!closed.get()) {
                        webSocket.send("#ping");
                    }
                } catch (Exception ignored) {
                    // OkHttp will surface errors via onFailure/onClosed
                }
            }, 12, 12, TimeUnit.SECONDS);

            if (!timeout) {
                messageObservable.onNext("투네이션에 연결되었습니다!");
            } else {
                timeout = false;
            }
        }

        @Override
        public void onMessage(WebSocket webSocket, String text) {
            if (text == null || text.isEmpty()) {
                return;
            }
            if ("#pong".equals(text)) {
                return;
            }
            if ("#block".equals(text)) {
                messageObservable.onNext("투네이션 소켓이 차단(block) 상태입니다.");
                return;
            }
            if (text.startsWith("#")) {
                return;
            }

            JSONParser parser = new JSONParser();
            try {
                JSONObject json = (JSONObject) parser.parse(text);
                Donation donation = Toonation.getDonation(json);
                if (donation != null) {
                    donationObservable.onNext(donation);
                }
            } catch (ParseException e) {
                messageObservable.onNext("투네이션 메시지 파싱 오류: " + e.getMessage());
            } catch (ClassCastException e) {
                messageObservable.onNext("투네이션 메시지 형식 오류: " + e.getMessage());
            }
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            ScheduledFuture<?> prev = pingTask;
            if (prev != null) {
                prev.cancel(true);
                pingTask = null;
            }
            if (!closed.get()) {
                messageObservable.onNext("투네이션 연결이 종료 되었습니다!");
            }
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable t, Response response) {
            if (closed.get()) {
                return;
            }

            timeout = true;
            messageObservable.onNext("투네이션 연결 오류: " + (t != null ? t.getMessage() : "unknown"));

            ScheduledFuture<?> prev = pingTask;
            if (prev != null) {
                prev.cancel(true);
                pingTask = null;
            }

            webSocket.close(1000, null);

            int attempt = reconnectAttempts.incrementAndGet();
            long delaySeconds = Math.min(30, (long) Math.pow(2, Math.min(attempt, 5)));
            scheduler.schedule(() -> {
                if (closed.get()) {
                    return;
                }
                socketRef.set(socketClient.newWebSocket(request, this));
            }, delaySeconds, TimeUnit.SECONDS);
        }
    }
}
