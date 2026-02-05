package com.outstandingboy.donationalert.platform;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
import com.outstandingboy.donationalert.exception.TwipVersionNotFoundException;

import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import io.socket.client.IO;
import io.socket.client.Socket;

public class Twip implements Platform {
    private Socket socket = null;
    private Subject<Donation> donationObservable;
    private Subject<String> messageObservable;

    public Twip(String key) throws IOException {
        Document doc = Jsoup.connect("https://twip.kr/widgets/alertbox/" + key).get();
        Elements scriptElements = doc.getElementsByTag("script");
        String script = scriptElements.stream().filter(e -> !e.hasAttr("src")).map(Element::toString).collect(Collectors.joining());

        String version = parseVersion(script);
        String token = parseToken(script);

        if (version == null) {
            throw new TwipVersionNotFoundException("버전을 찾을 수 없습니다.");
        }
        if (token == null) {
            throw new TokenNotFoundException("토큰을 찾을 수 없습니다.");
        }

        init(key, version, token);
    }

    public Twip(String key, String version, String token) {
        init(key, version, token);
    }

    private void init(String key, String version, String token) {
        donationObservable = PublishSubject.<Donation>create().toSerialized();
        // Connection events can fire before subscribeMessage().
        messageObservable = ReplaySubject.<String>createWithSize(16).toSerialized();

        String uri = String.format("https://io.mytwip.net?alertbox_key=" + key
            + "&version=" + version + "&token=" + encodeURIComponent(token));

        IO.Options opts = new IO.Options();
        String transports[] = {"websocket", "polling"};
        opts.transports = transports;
        opts.reconnection = true;

        try {
            socket = IO.socket(uri, opts);
        } catch (URISyntaxException e) {
            messageObservable.onNext("Twip 소켓 URI가 올바르지 않습니다: " + e.getMessage());
            throw new IllegalArgumentException("Invalid Twip socket URI", e);
        }

        socket.on(Socket.EVENT_CONNECT, (args) -> {
                messageObservable.onNext("트윕에 연결되었습니다!");
            })
            .on(Socket.EVENT_CONNECT_ERROR, (args) -> {
                messageObservable.onNext("연결 오류가 발생했습니다.");
            })
            .on("disconnect", (args) -> {
                messageObservable.onNext("연결이 종료되었습니다.");
            })
            .on("version not match", (args) -> {
                messageObservable.onNext("트윕 버전이 일치하지 않습니다.");
            })
            .on("not allowed ip", (args) -> {
                messageObservable.onNext("허용되지 않은 IP입니다.");
            })
            .on("new donate", (args) -> {
                JSONParser parser = new JSONParser();
                try {
                    JSONObject json = (JSONObject) parser.parse(args[0].toString());
                    Donation donation = getDonation(json);

                    if (donation != null) {
                        donationObservable.onNext(donation);
                    }
                } catch (ParseException e) {
                    messageObservable.onNext("Twip 후원 이벤트 파싱 오류: " + e.getMessage());
                }
            });
        socket.connect();
    }

    private String parseVersion(String script) {
        Pattern p = Pattern.compile("version: '(.*)'");
        Matcher m = p.matcher(script);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    private String parseToken(String script) {
        Pattern p = Pattern.compile("window.__TOKEN__ = '(.*)'");
        Matcher m = p.matcher(script);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    private Donation getDonation(JSONObject json) {
        try {
            Donation donation = new Donation();
            donation.setId((String) json.get("watcher_id"));
            donation.setNickName((String) json.get("nickname"));
            donation.setAmount((long) json.get("amount"));
            donation.setComment((String) json.get("comment"));
            return donation;
        } catch (Exception e) {
            return null;
        }
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
        donationObservable.onComplete();
        messageObservable.onComplete();
        if (socket != null) {
            socket.close();
        }
    }

    public static String encodeURIComponent(String s) {
        if (s == null) return "";

        // URLEncoder is close but not identical to JS encodeURIComponent.
        // Adjust to match common URI query encoding expectations.
        String encoded;
        try {
            encoded = URLEncoder.encode(s, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            return s;
        }

        return encoded
            .replace("+", "%20")
            .replace("%21", "!")
            .replace("%27", "'")
            .replace("%28", "(")
            .replace("%29", ")")
            .replace("%7E", "~");
    }
}
