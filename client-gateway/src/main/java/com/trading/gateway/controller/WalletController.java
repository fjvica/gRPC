package com.trading.gateway.controller;

import com.trading.wallet.BalanceRequest;
import com.trading.wallet.BalanceResponse;
import com.trading.wallet.WalletServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class WalletController {

    /**
     * Inyectamos el Stub ASÍNCRONO.
     * A diferencia del 'BlockingStub', este no detiene la ejecución del hilo principal.
     * En lugar de devolver un objeto, nos permite registrar "escuchadores" (callbacks)
     * que reaccionarán cuando el servidor gRPC nos envíe datos.
     */
    @GrpcClient("wallet-service")
    private WalletServiceGrpc.WalletServiceStub asyncStub;

    /**
     * Usamos TEXT_EVENT_STREAM_VALUE (Server-Sent Events) para que el navegador
     * sepa que la conexión se quedará abierta y recibirá trozos de datos (chunks)
     * en lugar de un JSON cerrado.
     */
    @GetMapping(value = "/stream-live/{id}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getLiveStream(@PathVariable String id) {

        /*
         * Flux.create es una factoría de Project Reactor que nos permite crear un flujo
         * de datos manual. El 'sink' (sumidero) es el objeto al que le iremos pasando
         * los datos que nos lleguen de gRPC para que los mande al navegador.
         */
        return Flux.create(sink -> {

            // Construimos la petición gRPC a partir del ID de la URL
            BalanceRequest request = BalanceRequest.newBuilder()
                    .setUserId(id)
                    .build();

            /*
             * Llamada al servidor gRPC usando el Stub Asíncrono.
             * Le pasamos la petición y un StreamObserver (un vigilante).
             */
            asyncStub.streamBalanceUpdates(request, new StreamObserver<BalanceResponse>() {

                /**
                 * Se ejecuta cada vez que el servidor gRPC hace un 'onNext'.
                 * Aquí es donde ocurre la magia: el dato llega de la red y
                 * "empujamos" ese dato al sink del Flux inmediatamente.
                 */
                @Override
                public void onNext(BalanceResponse value) {
                    sink.next("Saldo actualizado: " + value.getAmount() + " " + value.getCurrency());
                }

                /**
                 * Si algo falla en el servidor (ej. el puerto se cierra o hay error de red),
                 * notificamos al Flux para que el navegador también reciba el error.
                 */
                @Override
                public void onError(Throwable t) {
                    sink.error(t);
                }

                /**
                 * Cuando el servidor gRPC termina de enviar todos los datos (ej. después
                 * de los 5 mensajes), cerramos el flujo del navegador.
                 */
                @Override
                public void onCompleted() {
                    sink.complete();
                }
            });

            /*
             * IMPORTANTE: El metodo 'getLiveStream' termina aquí y devuelve el Flux vacío.
             * Spring mantiene la conexión HTTP abierta con el navegador esperando a que
             * los métodos onNext de arriba se vayan ejecutando en el futuro.
             */
        });
    }


    @GetMapping(value = "/monitor-test", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> monitorTest() {
        return Flux.create(sink -> {

            // 1. ABRIMOS EL CANAL: Le decimos al servidor "voy a empezar a mandarte cosas"
            // El StreamObserver de aquí dentro es para recibir lo que el servidor nos diga
            StreamObserver<BalanceRequest> requestObserver = asyncStub.monitorMultipleWallets(new StreamObserver<BalanceResponse>() {
                @Override
                public void onNext(BalanceResponse value) {
                    // El servidor nos responde un saldo -> lo mandamos al navegador
                    sink.next("Servidor dice: Saldo de " + value.getAmount());
                }

                @Override
                public void onError(Throwable t) { sink.error(t); }

                @Override
                public void onCompleted() { sink.complete(); }
            });

            // 2. ENVIAMOS LOS DATOS: Pero lo hacemos en un hilo aparte para no bloquear al navegador
            new Thread(() -> {
                try {
                    String[] users = {"User-A", "User-B", "User-C"};
                    for (String user : users) {
                        // Enviamos el ID al servidor gRPC
                        requestObserver.onNext(BalanceRequest.newBuilder().setUserId(user).build());
                        Thread.sleep(1000); // Esperamos 1 segundo entre envíos
                    }
                    // Avisamos que no mandaremos más IDs
                    requestObserver.onCompleted();
                } catch (Exception e) {
                    sink.error(e);
                }
            }).start();
        });
    }
}