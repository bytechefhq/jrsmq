package com.oblac.jrsmq;

import com.oblac.jrsmq.cmd.ChangeMessageVisibilityCmd;
import com.oblac.jrsmq.cmd.CreateQueueCmd;
import com.oblac.jrsmq.cmd.DeleteMessageCmd;
import com.oblac.jrsmq.cmd.DeleteQueueCmd;
import com.oblac.jrsmq.cmd.GetQueueAttributesCmd;
import com.oblac.jrsmq.cmd.ListQueuesCmd;
import com.oblac.jrsmq.cmd.PopMessageCmd;
import com.oblac.jrsmq.cmd.ReceiveMessageCmd;
import com.oblac.jrsmq.cmd.SendMessageCmd;
import com.oblac.jrsmq.cmd.SetQueueAttributesCmd;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

public class RedisSMQ {

	protected final RedisClient redisClient;
	protected final RedisSMQConfig config;

	public RedisSMQ() {
		this(RedisSMQConfig.createDefaultConfig());
	}

	public RedisSMQ(RedisSMQConfig config) {
		try {
			this.config = config;

			RedisURI redisURI = RedisURI.Builder
				.redis("localhost", 6379)
				.withDatabase(config.database())
				.withHost(config.host())
				.withPassword(config.password() == null ? null : config.password().toCharArray())
				.withPort(config.port())
				.withSsl(config.ssl())
				.withTimeout(Duration.of(config.timeout(), ChronoUnit.MILLIS))
				.build();

			redisClient = RedisClient.create(redisURI);

			initScript(redisClient);
		} catch (final Exception e) {
			quit();
			throw e;
		}
	}

	// ---------------------------------------------------------------- connect

	/**
	 * Gets a connection from the pool.
	 */
	protected RedisClient redisClient() {
		return redisClient;
	}

	/**
	 * Jedis supplier.
	 */
	protected Supplier<RedisClient> jedisSupplier = this::redisClient;

	// ---------------------------------------------------------------- cmds

	/**
	 * Changes the visibility timer of a single message.
	 * @see ChangeMessageVisibilityCmd
	 */
	public ChangeMessageVisibilityCmd changeMessageVisibility() {
		return new ChangeMessageVisibilityCmd(config, jedisSupplier, changeMessageVisibility);
	}
	/**
	 * Creates a new queue.
	 * @see CreateQueueCmd
	 */
	public CreateQueueCmd createQueue() {
		return new CreateQueueCmd(config, jedisSupplier);
	}

	/**
	 * Deletes a queue and all messages.
	 * @see DeleteQueueCmd
	 */
	public DeleteQueueCmd deleteQueue() {
		return new DeleteQueueCmd(config, jedisSupplier);
	}

	/**
	 * Deletes a message.
	 * @see DeleteMessageCmd
	 */
	public DeleteMessageCmd deleteMessage() {
		return new DeleteMessageCmd(config, jedisSupplier);
	}

	/**
	 * Returns queue attributes, counter and stats.
	 * @see GetQueueAttributesCmd
	 */
	public GetQueueAttributesCmd getQueueAttributes() {
		return new GetQueueAttributesCmd(config, jedisSupplier);
	}

	/**
	 * Sets queue parameters.
	 * @see SetQueueAttributesCmd
	 */
	public SetQueueAttributesCmd setQueueAttributes() {
		return new SetQueueAttributesCmd(config, jedisSupplier);
	}

	/**
	 * Lists all queues.
	 * @see ListQueuesCmd
	 */
	public ListQueuesCmd listQueues() {
		return new ListQueuesCmd(config, jedisSupplier);
	}

	/**
	 * Receives the next message from the queue and <b>deletes</b> it.
	 * @see PopMessageCmd
	 */
	public PopMessageCmd popMessage() {
		return new PopMessageCmd(config, jedisSupplier, popMessageSha1);
	}

	/**
	 * Receives the next message from the queue.
	 * @see ReceiveMessageCmd
	 */
	public ReceiveMessageCmd receiveMessage() {
		return new ReceiveMessageCmd(config, jedisSupplier, receiveMessageSha1);
	}

	/**
	 * Sends a new message.
	 * @see SendMessageCmd
	 */
	public SendMessageCmd sendMessage() {
		return new SendMessageCmd(config, jedisSupplier);
	}

	/**
	 * Disconnects the redis client.
	 */
	public void quit() {
		try {
			if (this.redisClient != null) {
				this.redisClient.close();
			}
		}
		catch (Exception ex) {
			// ignore
		}
	}

	// ---------------------------------------------------------------- scripts

	private static final String SCRIPT_POPMESSAGE = "local msg = redis.call(\"ZRANGEBYSCORE\", KEYS[1], \"-inf\", KEYS[2], \"LIMIT\", \"0\", \"1\") if #msg == 0 then return {} end redis.call(\"HINCRBY\", KEYS[1] .. \":Q\", \"totalrecv\", 1) local mbody = redis.call(\"HGET\", KEYS[1] .. \":Q\", msg[1]) local rc = redis.call(\"HINCRBY\", KEYS[1] .. \":Q\", msg[1] .. \":rc\", 1) local o = {msg[1], mbody, rc} if rc==1 then table.insert(o, KEYS[2]) else local fr = redis.call(\"HGET\", KEYS[1] .. \":Q\", msg[1] .. \":fr\") table.insert(o, fr) end redis.call(\"ZREM\", KEYS[1], msg[1]) redis.call(\"HDEL\", KEYS[1] .. \":Q\", msg[1], msg[1] .. \":rc\", msg[1] .. \":fr\") return o";
	private static final String SCRIPT_RECEIVEMESSAGE = "local msg = redis.call(\"ZRANGEBYSCORE\", KEYS[1], \"-inf\", KEYS[2], \"LIMIT\", \"0\", \"1\") if #msg == 0 then return {} end redis.call(\"ZADD\", KEYS[1], KEYS[3], msg[1]) redis.call(\"HINCRBY\", KEYS[1] .. \":Q\", \"totalrecv\", 1) local mbody = redis.call(\"HGET\", KEYS[1] .. \":Q\", msg[1]) local rc = redis.call(\"HINCRBY\", KEYS[1] .. \":Q\", msg[1] .. \":rc\", 1) local o = {msg[1], mbody, rc} if rc==1 then redis.call(\"HSET\", KEYS[1] .. \":Q\", msg[1] .. \":fr\", KEYS[2]) table.insert(o, KEYS[2]) else local fr = redis.call(\"HGET\", KEYS[1] .. \":Q\", msg[1] .. \":fr\") table.insert(o, fr) end return o";
	private static final String SCRIPT_CHANGEMESSAGEVISIBILITY = "local msg = redis.call(\"ZSCORE\", KEYS[1], KEYS[2]) if not msg then return 0 end redis.call(\"ZADD\", KEYS[1], KEYS[3], KEYS[2]) return 1";

	protected String popMessageSha1;
	protected String receiveMessageSha1;
	protected String changeMessageVisibility;

	protected void initScript(RedisClient redisClient) {
		try(StatefulRedisConnection<String, String> redisConnection = redisClient.connect()) {
			RedisCommands<String, String> redisCommands = redisConnection.sync();

			popMessageSha1 = redisCommands.scriptLoad(SCRIPT_POPMESSAGE);
			receiveMessageSha1 = redisCommands.scriptLoad(SCRIPT_RECEIVEMESSAGE);
			changeMessageVisibility = redisCommands.scriptLoad(SCRIPT_CHANGEMESSAGEVISIBILITY);
		}
	}

}
