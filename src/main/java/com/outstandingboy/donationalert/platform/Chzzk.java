package com.outstandingboy.donationalert.platform;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonObject;
import com.outstandingboy.donationalert.entity.Donation;
import com.outstandingboy.donationalert.util.Gsons;

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

public class Chzzk extends WebSocketListener implements Platform {
    private WebSocket socket;
    private Subject<Donation> donationObservable;
    private Subject<String> messageObservable;
    private boolean connected;
    private final String channelId;
    private final String chatChannelId;
    private final String accessToken;
    private final int wsId;
    private final OkHttpClient client;

    public Chzzk(String channelId) {
        this.channelId = channelId;
        this.wsId = Math.abs(channelId.chars().sum()) % 9 + 1;
        this.donationObservable = PublishSubject.<Donation>create().toSerialized();
        // Connection events can happen before subscribeMessage().
        this.messageObservable = ReplaySubject.<String>createWithSize(16).toSerialized();
        this.client = new OkHttpClient.Builder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .addInterceptor(chain -> {
                Request original = chain.request();
                Request authorized = original.newBuilder()
                    .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0")
                    .build();
                return chain.proceed(authorized);
            })
            .build();
        this.chatChannelId = getChatChannelId();
        accessToken = getAccessToken();
        connectToWebSocket();
    }

    private void connectToWebSocket() {
        Request request = new Request.Builder()
            .url("wss://kr-ss" + wsId + ".chat.naver.com/chat")
            .build();
        socket = client.newWebSocket(request, this);
    }

    private String getChatChannelId() {
        Request request = new Request.Builder()
            .url("https://api.chzzk.naver.com/service/v2/channels/" + channelId + "/live-detail")
            .get()
            .build();
        try (Response res = client.newCall(request).execute()) {
            ResponseBody body = res.body();
            if (res.isSuccessful() && body != null) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) Gsons.gson().fromJson(body.string(), Map.class);
                Object contentObj = map.get("content");
                if (contentObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> content = (Map<String, Object>) contentObj;
                    Object chatChannelIdObj = content.get("chatChannelId");
                    if (chatChannelIdObj != null) {
                        return chatChannelIdObj.toString();
                    }
                }
            }
            throw new RuntimeException("Failed to get Chat Channel ID from " + channelId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getAccessToken() {
        Request request = new Request.Builder()
            .url("https://comm-api.game.naver.com/nng_main/v1/chats/access-token?channelId=" + chatChannelId + "&chatType=STREAMING")
            .get()
            .build();
        try (Response res = client.newCall(request).execute()) {
            ResponseBody body = res.body();
            if (body == null) {
                throw new RuntimeException("Failed to get access token (empty body)");
            }
            JsonObject json = Gsons.gson().fromJson(body.string(), JsonObject.class);
            JsonObject content = json.get("content").getAsJsonObject();
            return content.get("accessToken").getAsString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onOpen(WebSocket webSocket, Response response) {
        if (!connected) {
            HashMap<String, Object> map = new HashMap<>();
            HashMap<String, Object> body = new HashMap<>();
            body.put("accTkn", accessToken);
            body.put("auth", "READ");
            body.put("uid", null);
            map.put("bdy", body);
            map.put("cmd", 100);
            map.put("tid", 1);
            map.put("ver", "2");
            map.put("cid", chatChannelId);
            map.put("svcid", "game");
            webSocket.send(Gsons.gson().toJson(map));
        }
    }

    @Override
    public void onMessage(WebSocket webSocket, String text) {
        JsonObject json = Gsons.gson().fromJson(text, JsonObject.class);
        int cmd = json.get("cmd").getAsInt();
        if (cmd == 10100) {
            connected = true;
            messageObservable.onNext("치지직에 연결되었습니다!");
        } else if (cmd == 93102) {
            JsonObject bdy = json.get("bdy").getAsJsonArray().get(0).getAsJsonObject();
            Donation donation = new Donation();
            JsonObject profile = Gsons.gson().fromJson(bdy.get("profile").getAsString(), JsonObject.class);
            JsonObject extras = Gsons.gson().fromJson(bdy.get("extras").getAsString(), JsonObject.class);
            donation.setId(profile.get("userIdHash").getAsString());
            donation.setAmount(extras.get("payAmount").getAsLong());
            donation.setNickName(profile.get("nickname").getAsString());
            donation.setComment(bdy.get("msg").getAsString());
            donationObservable.onNext(donation);
        }
    }

    @Override
    public void onClosed(WebSocket webSocket, int code, String reason) {
        messageObservable.onNext("치지직 연결이 종료 되었습니다!");
    }

    @Override
    public void onFailure(WebSocket webSocket, Throwable t, Response response) {
        webSocket.close(1000, null);
        connectToWebSocket();
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
            socket.close(1000, null);
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
}
