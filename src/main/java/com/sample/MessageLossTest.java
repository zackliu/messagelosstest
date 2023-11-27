package com.sample;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.azure.messaging.webpubsub.WebPubSubServiceClient;
import com.azure.messaging.webpubsub.WebPubSubServiceClientBuilder;
import com.azure.messaging.webpubsub.client.WebPubSubClient;
import com.azure.messaging.webpubsub.client.WebPubSubClientBuilder;
import com.azure.messaging.webpubsub.client.models.WebPubSubClientCredential;
import com.azure.messaging.webpubsub.models.GetClientAccessTokenOptions;
import com.azure.messaging.webpubsub.models.WebPubSubClientAccessToken;

import reactor.core.publisher.Mono;

public class MessageLossTest {
  private WebPubSubServiceClient webPubsubServiceHubClient;
  private int _sentId = 0;
  private int _receivedId = 0;

  private String clientGroup = "client";
  private String serverGroup = "server";

  public static void main(String[] args) throws InterruptedException {
    if (args.length != 1) {
      throw new IllegalArgumentException("Exactly 1 parameters required !");
    }
    System.out.println(args[0]);
    if (!args[0].equals("client") && !args[0].equals("server")) {
      throw new IllegalArgumentException("Args must be 'server' or 'client' !");
    }
    new MessageLossTest().run(args[0]);
  }

  private void run(String role) throws InterruptedException {
    webPubsubServiceHubClient = new WebPubSubServiceClientBuilder()
            .connectionString("")
            .hub("hub")
            .buildClient();

    WebPubSubClientCredential clientCredential = new WebPubSubClientCredential(GetToken());

    WebPubSubClient client = new WebPubSubClientBuilder()
            .credential(clientCredential)
            .buildClient();
    
    if (role.equals("client")) {
      RunAsClient(client);
    } else {
      RunAsServer(client);
    }

    TimeUnit.SECONDS.sleep(100);
  }

  private void RunAsClient(WebPubSubClient client) {
    System.out.println("0");
    client.addOnGroupMessageEventHandler(event -> {
      String data = event.getData().toString();
      int receivedId = Integer.parseInt(data);
      if (receivedId != _receivedId + 1) {
        System.out.println(String.format("Message LOSS! Should receive %d, but got %d", _receivedId+1, receivedId));
      }
      _receivedId = receivedId;
      System.out.println("Received");
    });

    System.out.println("1");
    client.start();
    System.out.println("2");
    client.joinGroup(clientGroup);

    client.sendToGroup(clientGroup, String.valueOf(_sentId));

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
      while (true)
      {
        _sentId = _sentId + 1;
        client.sendToGroup(clientGroup, String.valueOf(_sentId));
      }
      
    }, executorService);
    future.join();
  }

  private void RunAsServer(WebPubSubClient client) {

  }

  private Mono<String> GetToken() {
    String token = webPubsubServiceHubClient.getClientAccessToken(new GetClientAccessTokenOptions()
    .addRole("webpubsub.joinLeaveGroup")
    .addRole("webpubsub.sendToGroup")).getUrl();
    return Mono.just(token);
  }
}