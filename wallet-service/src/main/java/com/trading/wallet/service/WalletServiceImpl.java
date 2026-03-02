package com.trading.wallet.service;

import com.trading.wallet.BalanceRequest;
import com.trading.wallet.BalanceResponse;
import com.trading.wallet.WalletServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class WalletServiceImpl extends WalletServiceGrpc.WalletServiceImplBase {
    // Vacío aquí dentro

    @Override
    public void getBalance(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        String userId = request.getUserId();

        // Simulación de validación
        if ("666".equals(userId)) {
            responseObserver.onError(
                    io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("El usuario está bloqueado por el sistema de fraude")
                            .asRuntimeException()
            );
            return; // Importante salir del metodo
        }

        // Si all va bien...
        BalanceResponse response = BalanceResponse.newBuilder()
                .setAmount(100.0)
                .setCurrency("USD")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void streamBalanceUpdates(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        for (int i = 1; i <= 5; i++) {
            BalanceResponse response = BalanceResponse.newBuilder()
                    .setAmount(100.0 + (i * 10)) // El saldo sube mágicamente
                    .setCurrency("USD")
                    .build();

            responseObserver.onNext(response); // Enviamos una pieza de datos
            System.out.println("Enviando actualización " + i);

            try {
                Thread.sleep(1000); // Esperamos 1 segundo entre envíos
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        responseObserver.onCompleted(); // Cerramos el grifo
    }


    @Override
    public StreamObserver<BalanceRequest> monitorMultipleWallets(StreamObserver<BalanceResponse> responseObserver) {

        // PASO 1: "Hola Cliente, aquí tienes mi oreja (el observador) para que me hables"
        return new StreamObserver<BalanceRequest>() {

            @Override
            public void onNext(BalanceRequest request) {
                // PASO 2: El cliente me ha susurrado un ID de usuario al oído.
                System.out.println("Recibí: " + request.getUserId());

                // PASO 3: Yo (el servidor) uso la oreja del CLIENTE (responseObserver)
                // para responderle de inmediato.
                BalanceResponse resp = BalanceResponse.newBuilder()
                        .setAmount(500.0)
                        .build();

                responseObserver.onNext(resp);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Error en el stream del cliente: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                // PASO 4: El cliente me dice: "Ya no te voy a decir más nombres".
                // Entonces yo le respondo: "Vale, yo también cuelgo".
                responseObserver.onCompleted();
            }
        };
    }
}
