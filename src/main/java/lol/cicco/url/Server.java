package lol.cicco.url;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.redis.client.*;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import lol.cicco.url.util.ConversionUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;

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
                .setDatabase("short_url")
                .setUser("postgres")
                .setPassword("postgres");

        // Pool options
        PoolOptions poolOptions = new PoolOptions().setMaxSize(5);


        // Create the pooled client
        pgClient = PgPool.pool(vertx, connectOptions, poolOptions);

        // Redis Client Document
        // https://vertx.io/docs/vertx-redis-client/java/
        redis = Redis.createClient(
                vertx,
                new RedisOptions()
                        .setConnectionString("redis://localhost:6379")
                        .setMaxPoolSize(8)
                        .setMaxWaitingHandlers(32))
                .send(Request.cmd(Command.PING), send -> {
                    if (send.succeeded()) {
                        // Should have received a pong...
                        Response response = send.result();
                        log.debug("redis ping received is [{}]", response.toString());
                    } else {
                        log.warn("无法连接到Redis...");
                    }
                });
    }

    @Override
    public void start() throws Exception {
        Router router = Router.router(vertx);
        router.get("/:shortUrl").handler(routingContext -> {
            String shortUrl = routingContext.pathParam("shortUrl");
            redis.connect(redisConnHandler -> {
                if(redisConnHandler.succeeded()) {
                    var redisConn = redisConnHandler.result();
                    redisConn.send(Request.cmd(Command.GET).arg(shortUrl), getHandler -> {
                        if(getHandler.succeeded()) {
                            HttpServerResponse response = routingContext.response();
                            var setExResponse = getHandler.result();
                            if(setExResponse == null) {
                                log.warn("GET shortUrl cache has some error... shortUrl is {}", shortUrl);
                                response.setStatusCode(404).end();
                            } else {
                                log.debug("GET shortURL cache {}", setExResponse.toString());
                                response.putHeader("location", setExResponse.toString());
                                response.setStatusCode(302).end();
                            }
                        }
                        redisConn.close();
                    });
                }
            });
        });

        router.put("/create").handler(context -> {
            String longUrl = context.request().getParam("url");
            pgClient.getConnection(connHandler -> {
                if (connHandler.succeeded()) {
                    var conn = connHandler.result();
                    conn.preparedQuery("select nextval('url_idx_seq') as next_val").execute(queryHandler -> {
                        if(queryHandler.succeeded()) {
                            var result = queryHandler.result();
                            long nextVal = result.iterator().next().getLong("next_val");

                            String shortUrl = ConversionUtils.encode(nextVal);

                            conn.preparedQuery("insert into short_url(id, short_url, long_url, create_time) values ($1, $2, $3, $4)").execute(
                                    Tuple.of(nextVal, shortUrl, longUrl, LocalDateTime.now()), insertHandler -> {
                                        if(insertHandler.succeeded()) {
                                            RowSet<Row> rows = insertHandler.result();
                                            log.debug("Save short url rows : {}", rows.rowCount());
                                        }
                                        conn.close();

                                        redis.connect(redisConnHandler -> {
                                            if(redisConnHandler.succeeded()) {
                                                var redisConn = redisConnHandler.result();
                                                redisConn.send(Request.cmd(Command.SETEX).arg(shortUrl).arg(60*60).arg(longUrl), setExHandler -> {
                                                    if(setExHandler.succeeded()) {
                                                        var setExResponse = setExHandler.result();
                                                        if(setExResponse == null) {
                                                            log.warn("SETEX has some error...");
                                                        } else {
                                                            log.debug("SETEX result : {}", setExResponse.toString());
                                                        }
                                                    }
                                                    redisConn.close();
                                                });
                                            }
                                        });

                                        JsonObject respResult = new JsonObject();
                                        respResult.put("status", 200);
                                        respResult.put("short_url", "http://localhost:8888/" + shortUrl);
                                        context.response().end(respResult.toString());
                                    }
                            );
                        }
                    });
                } else {
                    log.warn("获取数据库连接失败..");
                }
            });
        });

        // Web Document https://vertx.io/docs/vertx-web/java/
        vertx.createHttpServer().requestHandler(router).listen(8888);
    }

    @Override
    public void stop() throws Exception {
        pgClient.close();
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new Server(), handler -> {
            if(handler.succeeded()) {
                log.info("DeployVerticle Succeeded.");
            } else {
                log.info("DeployVerticle Failed.");
            }
        });
    }
}
