package lol.cicco.url;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.redis.client.*;
import io.vertx.sqlclient.PoolOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Server extends AbstractVerticle {
    private PgPool pgClient;
    private Redis redis;

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);

        // Reactive Pg Client Document
        // https://vertx.io/docs/vertx-pg-client/java
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(5432)
                .setHost("127.0.0.1")
                .setDatabase("postgres")
                .setUser("postgres")
                .setPassword("zhaoxu@2020");

        // Pool options
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(5);

        // Create the pooled client
        pgClient = PgPool.pool(vertx, connectOptions, poolOptions);

        // Redis Client Document
        // https://vertx.io/docs/vertx-redis-client/java/
        redis = Redis.createClient(
                vertx,
                new RedisOptions()
                        .setConnectionString("redis://localhost:6379")
                        // allow at max 8 connections to redis
                        .setMaxPoolSize(8)
                        // allow 32 connection requests to queue waiting
                        // for a connection to be available.
                        .setMaxWaitingHandlers(32))
                .send(Request.cmd(Command.PING), send -> {
                    if (send.succeeded()) {
                        // Should have received a pong...
                        Response response = send.result();
                        System.out.println(response.toString());
                    } else {
                        log.warn("无法连接到Redis...");
                    }
                });
    }

    @Override
    public void start() throws Exception {
        Router router = Router.router(vertx);

        router.get("/test").handler(handler -> {
            handler.response().end("test!!!!");
        });

        // Web Document https://vertx.io/docs/vertx-web/java/
        vertx.createHttpServer().requestHandler(router).listen(8888);

        pgClient.getConnection(connHandler -> {
            if (connHandler.succeeded()) {
                var conn = connHandler.result();
                conn.query("select 1 as count").execute(queryHandler -> {
                    if(queryHandler.succeeded()) {
                        var result = queryHandler.result();
                        result.forEach(row -> {
                            System.out.println(row.getInteger("count"));
                        });
                    }
                    conn.close();
                });
            }
        });

        redis.connect(connHandler -> {
            if(connHandler.succeeded()) {
                var conn = connHandler.result();
                conn.send(Request.cmd(Command.GET).arg("myKey"), getHandler -> {
                    if(getHandler.succeeded()) {
                        var result = getHandler.result();
                        if(result == null) {
                            log.warn("myKey is null...");
                        } else {
                            System.out.println(result.toString());
                        }
                    } else {
                        log.warn("get myKey error..");
                    }
                });
            }
        });
    }

    @Override
    public void stop() throws Exception {
        pgClient.close();
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new Server());
    }
}
