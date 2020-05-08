package lol.cicco.url;


import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.redis.client.RedisOptions;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.pgclient.PgPool;
import io.vertx.rxjava.redis.client.Command;
import io.vertx.rxjava.redis.client.Redis;
import io.vertx.rxjava.redis.client.Request;
import io.vertx.rxjava.redis.client.Response;
import io.vertx.rxjava.sqlclient.Row;
import io.vertx.rxjava.sqlclient.RowSet;
import io.vertx.rxjava.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;
import lol.cicco.url.util.ConversionUtils;
import lombok.extern.slf4j.Slf4j;
import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.functions.Func1;

@Slf4j
public class RxServer extends AbstractVerticle {
    private PgPool pgClient;
    private Redis redis;

    @Override
    public void init(Vertx coreVertx, Context coreContext) {
        super.init(coreVertx, coreContext);

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
    public Completable rxStart() {
        Router router = Router.router(vertx);
        router.get("/:shortUrl").handler(context -> {
            var url = context.pathParam("shortUrl");
            redis.rxConnect().flatMap(conn -> {
                var send = conn.rxSend(Request.cmd(Command.GET).arg(url));
                return send.doAfterTerminate(conn::close);
            }).flatMap(response -> {
                if(response != null) {
                    return Single.just(response.toString());
                }
                long id = ConversionUtils.decode(url);
                if(id == 0) {
                    return Single.just(null);
                }
                return pgClient.rxGetConnection().flatMap(
                        sqlConnection ->
                                sqlConnection.preparedQuery("select long_url from url_record where id = $1")
                                        .rxExecute(Tuple.of(id))
                                        .doAfterTerminate(sqlConnection::close)
                ).map(rows -> {
                    if(rows.rowCount() == 0) {
                        return null;
                    }
                    var originUrl = rows.iterator().next().getString("long_url");
                    saveToRedis(id, originUrl);
                    return originUrl;
                });
            }).subscribe(res -> {
                var response = context.response();
                if(res == null) {
                    response.setStatusCode(404).end();
                } else {
                    response.putHeader("location", res);
                    response.setStatusCode(302).end();
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
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new RxServer(), handler -> {
            if(handler.succeeded()) {
                log.info("DeployVerticle Succeeded.");
            } else {
                log.info("DeployVerticle Failed.");
            }
        });
    }
}
