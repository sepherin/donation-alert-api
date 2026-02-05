# 💸 Donation Alert API
[Twip](http://twip.kr/), [Toonation](https://toon.at/)의 후원 알림(Alertbox)을 받아올 수 있는 RxJava 기반 Java API  

![so much money](https://media.giphy.com/media/3orif1esInVTdhaNsk/giphy.gif)

> [outstandingboy/donation-alert-api](https://github.com/outstanding1301/donation-alert-api)  

----

# 🚀 Start
## Gradle (use [Jitpack](https://jitpack.io/))
```gradle
repositories {
    ...
    maven { url 'https://jitpack.io' }
}

dependencies {
    ...
    compile 'com.github.outstanding1301:donation-alert-api:1.0.0'
}
```

```maven
<repositories>
    ...
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
    ...
</repositories>

<dependency>
    ...
    <groupId>com.github.outstanding1301</groupId>
    <artifactId>donation-alert-api</artifactId>
    <version>1.0.0</version>
    ...
</dependency>
```

## Twip
```java
// Twip Alertbox URL의 마지막 https://twip.kr/widgets/alertbox/<YOUR_TWIP_KEY> 부분을 입력하세요.
Twip twip = new Twip("YOUR_TWIP_KEY");

// 메시지를 구독합니다.
// 연결 알림, 에러 등의 String 메시지를 처리하는 핸들러를 인자로 사용합니다. 
twip.subscribeMessage(s -> System.out.println(s));

// 도네이션 알림을 구독합니다.
// Donation 객체를 처리하는 핸들러를 인자로 사용합니다.
twip.subscribeDonation(donation -> {
    System.out.println("[Twip] "+donation.getNickName()+"님이 "+donation.getAmount()+"원을 후원했습니다.");
    System.out.println("후원 내용: "+donation.getComment());
});
```

## Toonation
```java
// Toonation Alertbox URL의 마지막 https://toon.at/widget/alertbox/<YOUR_TOONATION_KEY> 부분을 입력하세요.
Toonation toonation = new Toonation("YOUR_TOONATION_KEY");

// 메시지를 구독합니다.
// 연결 알림, 에러 등의 String 메시지를 처리하는 핸들러를 인자로 사용합니다. 
toonation.subscribeMessage(s -> System.out.println(s));

// 도네이션 알림을 구독합니다.
// Donation 객체를 처리하는 핸들러를 인자로 사용합니다.
toonation.subscribeDonation(donation -> {
    System.out.println("[Toonation] "+donation.getNickName()+"님이 "+donation.getAmount()+"원을 후원했습니다.");
    System.out.println("후원 내용: "+donation.getComment());
});
```

# 📃 Docs

## Donation

| 식별자 | 타입 | 설명 |
|:---:|:---:|:---:|
| id | String | 후원자 ID |
| nickname | String | 후원자 닉네임 |
| comment | String | 후원 내용 |
| amount | Integer | 후원 금액 |

<br>

## Platform (Twip, Toonation)

| 식별자 | 타입 | 설명 |
|:---:|:---:|:---:|
| subscribeDonation(Consumer<Donation> onNext) | void | 후원 알림 구독 |
| subscribeMessage(Consumer<String> onNext) | void | API 메시지 구독 |
| close() | void | 연결 종료 |
| getDonationObservable() | Subject<Donation> | 후원 알림 Subject 객체 반환 |
| getMessageObservable() | Subject<String> | API 메시지 Subject 객체 반환 |

# 💉 Dependencies
```gradle
implementation 'io.socket:socket.io-client:1.0.0'
implementation  'io.reactivex.rxjava2:rxjava:2.1.16'
implementation 'org.jsoup:jsoup:1.13.1'
implementation 'com.googlecode.json-simple:json-simple:1.1.1'
```

<br><br><br><br><br>

# 개발 환경

- OS: Windows (테스트/개발 환경)
- JDK: Java 8 소스 호환 (`sourceCompatibility = 1.8`)
    - 로컬 실행 테스트: Eclipse Temurin JDK 21
- Build Tool: Gradle Wrapper (`gradlew.bat`)

## 실행 파일(JAR) 빌드

### 1) 라이브러리 JAR 빌드 (의존성 미포함)

```powershell
./gradlew.bat clean build
```

- 결과물: `build/libs/DonationAlertAPI-<version>.jar`

### 2) 실행 가능한 Fat JAR 빌드 (의존성 포함)

프로젝트에는 실행 예제용 `main()`이 포함되어 있습니다: `com.outstandingboy.donationalert.ToonationAPI`.

```powershell
./gradlew.bat clean fatJar
```

- 결과물: `build/libs/DonationAlertAPI-<version>-all.jar`

실행:

```powershell
java -jar build/libs/DonationAlertAPI-<version>-all.jar
```

![alt text](<img/ScreenShot 2026-02-05 143538.png>)
