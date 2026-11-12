package repro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The client is given two brokers: an unusable endpoint first, then a live broker. Every attempt should end connected
 * to the live broker. The default load-balancing policy starts on a random connector, so each case makes attempts,
 * each with a new ServerLocator, until the unusable endpoint has been tried first {@value #TRIED_FIRST} times.
 * <p>
 * Each case runs without credentials, and with them, as the client then takes a different path: it sends a
 * ConnectMessage during {@code connect()} instead of only waiting for the topology afterwards.
 */
class InitialConnectFallThroughTest {

   private static final int TRIED_FIRST = 5;
   private static final int MAX_ATTEMPTS = 60;

   private static EmbeddedActiveMQ broker;
   private static int livePort;

   @BeforeAll
   static void startBroker() throws Exception {
      livePort = freePort();
      broker = new EmbeddedActiveMQ().setConfiguration(new ConfigurationImpl()
         .setPersistenceEnabled(false)
         .setSecurityEnabled(false)
         .addAcceptorConfiguration("live", "tcp://127.0.0.1:" + livePort));
      broker.start();
   }

   @AfterAll
   static void stopBroker() throws Exception {
      broker.stop();
   }

   /** Nothing listens on the first port, so the TCP connect is refused. Passes on 2.57.0. */
   @ParameterizedTest(name = "credentials={0}")
   @ValueSource(booleans = {false, true})
   void fallsThroughWhenConnectionIsRefused(boolean credentials) throws Exception {
      // A refused connect leaves no trace to count, so this makes a fixed number of attempts.
      assertEveryAttemptConnects(freePort(), null, credentials);
   }

   /** The first port accepts the TCP connection, then closes it at once, as a service mesh proxy does when it can't reach the backend. Fails on 2.57.0. */
   @ParameterizedTest(name = "credentials={0}")
   @ValueSource(booleans = {false, true})
   void fallsThroughWhenConnectionIsAcceptedThenClosed(boolean credentials) throws Exception {
      try (ServerSocket proxy = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
         AtomicInteger accepted = serve(proxy, Socket::close);
         assertEveryAttemptConnects(proxy.getLocalPort(), accepted, credentials);
      }
   }

   /** The first port accepts the TCP connection, then never answers. Fails on 2.57.0. */
   @ParameterizedTest(name = "credentials={0}")
   @ValueSource(booleans = {false, true})
   void fallsThroughWhenConnectionIsAcceptedThenSilent(boolean credentials) throws Exception {
      List<Socket> held = new ArrayList<>();
      try (ServerSocket proxy = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
         AtomicInteger accepted = serve(proxy, held::add);
         assertEveryAttemptConnects(proxy.getLocalPort(), accepted, credentials);
      } finally {
         for (Socket s : held) {
            s.close();
         }
      }
   }

   private static void assertEveryAttemptConnects(int unusablePort, AtomicInteger acceptedByProxy, boolean credentials) throws Exception {
      // callTimeout is lowered from its 30s default only to keep the silent case short.
      String url = "(tcp://127.0.0.1:" + unusablePort + ",tcp://127.0.0.1:" + livePort + ")?callTimeout=3000";
      System.out.printf("%n%s, credentials=%s%n", url, credentials);
      List<String> failures = new ArrayList<>();
      int triedFirst = 0;
      for (int i = 1; i <= MAX_ATTEMPTS && (acceptedByProxy == null ? i <= 2 * TRIED_FIRST : triedFirst < TRIED_FIRST); i++) {
         int acceptedBefore = acceptedByProxy == null ? 0 : acceptedByProxy.get();
         long start = System.nanoTime();
         String outcome;
         try (ServerLocator locator = ActiveMQClient.createServerLocator(url);
              ClientSessionFactory factory = credentials ? locator.createSessionFactory("user", "password") : locator.createSessionFactory()) {
            outcome = "connected to port " + factory.getConnectorConfiguration().getParams().get("port");
         } catch (Exception e) {
            outcome = e.getClass().getSimpleName() + ": " + e.getMessage();
            failures.add("attempt " + i + ": " + outcome);
         }
         boolean proxyTried = acceptedByProxy != null && acceptedByProxy.get() > acceptedBefore;
         if (proxyTried) {
            triedFirst++;
         }
         System.out.printf("attempt %d (%d ms): %s%s%n", i, (System.nanoTime() - start) / 1_000_000, outcome,
            proxyTried ? " (tried the fake endpoint)" : "");
      }
      assertEquals(List.of(), failures, "every attempt should reach the live broker on port " + livePort);
   }

   private interface SocketHandler {
      void handle(Socket socket) throws IOException;
   }

   private static AtomicInteger serve(ServerSocket proxy, SocketHandler handler) {
      AtomicInteger accepted = new AtomicInteger();
      Thread thread = new Thread(() -> {
         while (!proxy.isClosed()) {
            try {
               Socket socket = proxy.accept();
               accepted.incrementAndGet();
               handler.handle(socket);
            } catch (IOException e) {
               return;
            }
         }
      }, "fake-endpoint");
      thread.setDaemon(true);
      thread.start();
      return accepted;
   }

   private static int freePort() throws IOException {
      try (ServerSocket socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
         return socket.getLocalPort();
      }
   }
}
