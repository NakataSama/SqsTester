package sqs;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class SqsTester {

    String queueName = "test";

    public void run() {
        //Construindo um sqsClient
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://localhost:4566"))
                .build();

        //Construindo uma fila
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        sqsClient.createQueue(createQueueRequest);

        // Pegando url da fila
        String queueName = "test";
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

        String queueUrl = sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl();

        // Você também pode instanciar o Request diretamente no parâmetro
        //queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
        //        .queueName(queueName)
        //        .build()).queueUrl();

        // Enviando mensagem
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("Hello World!")
                .delaySeconds(1)
                .build());

        // Enviando mensagens em batch
        SendMessageBatchRequestEntry entry1 = SendMessageBatchRequestEntry.builder()
                .id("1")
                .messageBody("Hello World Batch 1")
                .build();
        SendMessageBatchRequestEntry entry2 = SendMessageBatchRequestEntry.builder()
                .id("2")
                .messageBody("Hello World Batch 2")
                .build();
        SendMessageBatchRequestEntry entry3 = SendMessageBatchRequestEntry.builder()
                .id("3")
                .messageBody("Hello World Batch 3")
                .build();

        List<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();

        entries.add(entry1);
        entries.add(entry2);
        entries.add(entry3);

        SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(entries)
                .build();

        sqsClient.sendMessageBatch(sendMessageBatchRequest);

        // Recebendo mensagens
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(2)
                .maxNumberOfMessages(10)
                .build();

        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

        // Printando body de todas as mensagens
        messages.forEach(message -> {
            System.out.println(message.body());
        });

        // Deletando as mensagens da fila
        messages.forEach(message -> {
            System.out.println("Deletando a mensagem " + message.messageId());
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
        });

        sqsClient.close();
    }
}
