package com.outstandingboy.donationalert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import com.outstandingboy.donationalert.platform.Toonation;

public final class ToonationAPI {

    public static void main(String[] args) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        String key = (args != null && args.length > 0) ? args[0] : null;
        Toonation toonation = null;
        while (toonation == null) {
            while (key == null || key.trim().isEmpty()) {
                System.out.print("Toonation API 키를 입력하세요 (https://toon.at/widget/alertbox/<KEY>): ");
                key = in.readLine();
                if (key == null) {
                    System.out.println("입력이 종료되었습니다.");
                    return;
                }
                key = key.trim();
            }

            try {
                toonation = new Toonation(key);
            } catch (IOException e) {
                System.out.println("[Toonation] 키가 올바르지 않거나(또는 네트워크 문제) 페이로드를 찾지 못했습니다.");
                System.out.println("[Toonation] 원인: " + e.getMessage());
                key = null; // 재입력 유도
            }
        }
        final Toonation created = toonation;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                created.close();
            } catch (Exception ignored) {
                // no-op
            }
        }, "toonation-shutdown"));

        try {
            try {
                if (!created.awaitConnected(5, TimeUnit.SECONDS)) {
                    System.out.println("[Toonation] 5초 내에 연결되지 않았습니다. 키/네트워크/방화벽을 확인하세요.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // 메시지를 구독합니다.
            // 연결 알림, 에러 등의 String 메시지를 처리하는 핸들러를 인자로 사용합니다.
            created.subscribeMessage(s -> System.out.println(s));

            // 도네이션 알림을 구독합니다.
            // Donation 객체를 처리하는 핸들러를 인자로 사용합니다.
            created.subscribeDonation(donation -> {
                System.out.println("[Toonation] " + donation.getNickName() + "님이 " + donation.getAmount() + "원을 후원했습니다.");
                System.out.println("후원 내용: " + donation.getComment());


            });

            System.out.println("종료하려면 Enter 키를 누르세요.");
            in.readLine();
        } finally {
            created.close();
        }
    }
}
