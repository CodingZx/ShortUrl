package lol.cicco.url;


import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisOptions;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.ext.jdbc.JDBCClient;
import io.vertx.rxjava.ext.sql.SQLClient;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.redis.client.Command;
import io.vertx.rxjava.redis.client.Redis;
import io.vertx.rxjava.redis.client.Request;
import io.vertx.rxjava.redis.client.Response;
import lol.cicco.url.util.ConversionUtils;
import lombok.extern.slf4j.Slf4j;
import rx.Completable;
import rx.Single;
import rx.functions.Action1;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
public class RxServer extends AbstractVerticle {
    private static final String PREFIX = "http://127.0.0.1:8888/";
    private SQLClient sqlClient;
    private Redis redis;

    @Override
    public void init(Vertx coreVertx, Context coreContext) {
        super.init(coreVertx, coreContext);

        JsonObject config = new JsonObject()
                .put("provider_class", "io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider")
                .put("jdbcUrl", "jdbc:postgresql://127.0.0.1:5432/short_url")
                .put("driverClassName", "org.postgresql.Driver")
                .put("username", "postgres")
                .put("password", "zhaoxu@2020")
                .put("initial_pool_size", 1)
                .put("maximumPoolSize", 30)
                .put("max_idle_time", 30);

        sqlClient = JDBCClient.create(vertx, config);

//        // Reactive Pg Client Document
//        // https://vertx.io/docs/vertx-pg-client/java
//        PgConnectOptions connectOptions = new PgConnectOptions()
//                .setPort(5432)
//                .setHost("127.0.0.1")
//                .setDatabase("short_url")
//                .setUser("postgres")
//                .setPassword("zhaoxu@2020");
//
//        // Pool options
//        PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
//
//        // Create the pooled client
//        pgClient = PgPool.pool(vertx, connectOptions, poolOptions);

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
    public Completable rxStart() {
        Router router = Router.router(vertx);
        // forward router
        router.get("/:shortUrl").handler(context -> {
            var url = context.pathParam("shortUrl");
            redis.rxConnect().flatMap(conn -> {
                var send = conn.rxSend(Request.cmd(Command.GET).arg(url));
                return send.doAfterTerminate(conn::close);
            }).flatMap(response -> {
                if (response != null) {
                    return Single.just(response.toString());
                }
                long id = ConversionUtils.decode(url);
                if (id == 0) {
                    return Single.just(null);
                }
                return sqlClient.rxGetConnection().flatMap(
                        sqlConnection ->
                                sqlConnection.rxQueryWithParams("select long_url from url_record where id = $1", new JsonArray().add(id))
                                        .doAfterTerminate(sqlConnection::close)
                ).map(rs -> {
                    if (rs == null) {
                        return null;
                    }
                    var originUrl = rs.toJson().getString("long_url");
                    saveToRedis(id, URLEncoder.encode(originUrl, StandardCharsets.UTF_8));
                    return originUrl;
                });
            }).subscribe(res -> {
                var response = context.response();
                if (res == null) {
                    response.setStatusCode(404).end();
                } else {
                    response.putHeader("location", URLDecoder.decode(res, StandardCharsets.UTF_8));
                    response.setStatusCode(302).end();
                }
            });
        });

        // create router
        router.post("/create").handler(context -> {

            Action1<String> responseError = (msg) -> {
                context.response().setStatusCode(200).write(new JsonObject().put("code", 400).put("msg", msg).toString()).end();
            };

            var paramUrl = context.request().getParam("url");
            if (paramUrl == null || paramUrl.isBlank()) {
                responseError.call("URL参数不能为空");
                return;
            }

            Action1<Long> responseSuccess = id -> {
                JsonObject result = new JsonObject();
                result.put("code", 200).put("msg", "success").put("shortUrl", PREFIX + ConversionUtils.encode(id));
                context.response().setStatusCode(200).write(result.toString()).end();
            };


            final String originUrl = URLEncoder.encode(paramUrl, StandardCharsets.UTF_8);

            var saveOriginUrl = sqlClient.rxGetConnection().flatMap(sqlConnection -> sqlConnection.rxQuery("select nextval('url_idx_seq') as seq").flatMap(resultSet -> {
                var nextIdx = resultSet.getRows().get(0).getLong("seq");

                var insert = sqlConnection.rxUpdateWithParams("insert into url_record(id, long_url, create_time) values(?, ?, '" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "')", new JsonArray().add(nextIdx).add(originUrl));
                saveToRedis(nextIdx, originUrl);
                insert.doAfterTerminate(() -> sqlConnection.rxCommit().subscribe(r -> sqlConnection.close()));
                return insert.map(f -> nextIdx);
            }));

            redis.rxConnect().flatMap(redisConnection -> {
                var send = redisConnection.rxSend(Request.cmd(Command.GET).arg(originUrl));
                return send.doAfterTerminate(redisConnection::close);
            }).subscribe(response -> {
                if (response == null) {
                    saveOriginUrl.doOnError(err -> {
                        log.error(err.getMessage(), err);
                        responseError.call("系统异常");
                    }).subscribe(responseSuccess);
                } else {
                    responseSuccess.call(Long.parseLong(response.toString()));
                }
            });
        });
        return vertx.createHttpServer().requestHandler(router).rxListen(8888).toCompletable();
    }

    private void saveToRedis(long id, String originUrl) {
        redis.rxConnect().flatMap(conn -> {
            var send = conn.rxSend(Request.cmd(Command.SETEX).arg(ConversionUtils.encode(id)).arg(60 * 60).arg(originUrl));
            return send.doAfterTerminate(conn::close);
        }).subscribe();
        redis.rxConnect().flatMap(conn -> {
            var send = conn.rxSend(Request.cmd(Command.SETEX).arg(originUrl).arg(60 * 60).arg(id));
            return send.doAfterTerminate(conn::close);
        }).subscribe();
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new RxServer(), handler -> {
            if (handler.succeeded()) {
                log.info("DeployVerticle Succeeded.");
            } else {
                log.info("DeployVerticle Failed.");
            }
        });
    }
}
