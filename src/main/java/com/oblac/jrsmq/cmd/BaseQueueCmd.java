package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.QueueDef;
import com.oblac.jrsmq.QueueMessage;
import com.oblac.jrsmq.RedisSMQConfig;
import com.oblac.jrsmq.RedisSMQException;
import com.oblac.jrsmq.Util;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;
import java.util.function.Supplier;

import static com.oblac.jrsmq.Values.Q;

public abstract class BaseQueueCmd<T> implements Cmd<T> {

	protected final RedisSMQConfig config;
	private final Supplier<RedisClient> redisClientSupplier;

	public BaseQueueCmd(RedisSMQConfig config, Supplier<RedisClient> redisClientSupplier) {
		this.config = config;
		this.redisClientSupplier = redisClientSupplier;
	}

	/**
	 * Opens RedisClient connection before usage, {@link #exec(RedisCommands<String, String>) executes command}
	 * and closes RedisClient connection.
	 */
	@Override
	public final T exec() {
		RedisClient redisClient = redisClientSupplier.get();

		try (StatefulRedisConnection<String, String> redisConnection = redisClient.connect()) {
			return exec(redisConnection.sync());
		}
	}

	/**
	 * Runs commands with given RedisCommands instance.
	 */
	protected abstract T exec(RedisCommands<String, String> redisCommands);

	/**
	 * Reads a queue from the Redis.
	 */
	protected QueueDef getQueue(RedisCommands<String, String> redisCommands, String qname, boolean generateUid) {
		redisCommands.multi();

		String key = config.redisNs() + qname + Q;

		redisCommands.hmget(key, "vt", "delay", "maxsize");
		redisCommands.time();

		TransactionResult transactionResult = redisCommands.exec();

		List<Object> results = transactionResult.stream().toList();

		List<KeyValue<String, String>> respGet = (List<KeyValue<String, String>>) results.get(0);

		if (respGet.get(0) == null || respGet.get(1) == null || respGet.get(2) == null) {
			throw new RedisSMQException("Queue not found: " + qname);
		}

		List<String> respTime = (List<String>) results.get(1);

		String ms = Util.formatZeroPad(respTime.get(1), 6);
		long ts = Long.valueOf(respTime.get(0) + ms.substring(0, 3));

		String id = null;
		if (generateUid) {
			id = Util.makeId(22);
			id = Long.toString(Long.valueOf(respTime.get(0) + ms), 36) + id;
		}

		return new QueueDef(
			qname, getValue(respGet.get(0)), getValue(respGet.get(1)), getValue(respGet.get(2)), ts, id);
	}

	/**
	 * Creates a queue message from resulting list.
	 */
	protected QueueMessage createQueueMessage(List<?> result) {
		if (result.isEmpty()) {
			return null;
		}

		return new QueueMessage(
			(String) result.get(0),
			(String) result.get(1),
			(Long) result.get(2),
			Long.parseLong((String)result.get(3)),
			Long.valueOf(((String)result.get(0)).substring(0, 10), 36) / 1000
		);
	}

	private String getValue(KeyValue<String, String> keyValue) {
		return keyValue.hasValue() ? keyValue.getValue() : null;
	}
}