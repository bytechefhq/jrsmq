package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.RedisSMQConfig;
import com.oblac.jrsmq.RedisSMQException;
import com.oblac.jrsmq.Validator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;
import java.util.function.Supplier;

import static com.oblac.jrsmq.Util.toInt;
import static com.oblac.jrsmq.Values.Q;
import static com.oblac.jrsmq.Values.QUEUES;

/**
 * Create a new queue.
 */
public class CreateQueueCmd extends BaseQueueCmd<Integer> {

	private int vt = 30;
	private int delay = 0;
	private int maxsize = 65536;
	private String qname;

	public CreateQueueCmd(RedisSMQConfig config, Supplier<RedisClient> jedisSupplier) {
		super(config, jedisSupplier);
	}

	/**
	 * The Queue name. Maximum 160 characters; alphanumeric characters, hyphens (-), and underscores (_) are allowed.
	 */
	public CreateQueueCmd qname(String qname) {
		this.qname = qname;
		return this;
	}

	/**
	 * Optional (Default: 30) length of time, in seconds, that a message received from a
	 * queue will be invisible to other receiving components when they ask to receive messages. Allowed values: 0-9999999 (around 115 days).
	 */
	public CreateQueueCmd vt(int vt) {
		this.vt = vt;
		return this;
	}

	/**
	 * Optional (Default: 0) time in seconds that the delivery of all new messages in the queue will be delayed.
	 * Allowed values: 0-9999999 (around 115 days)
	 */
	public CreateQueueCmd delay(int delay) {
		this.delay = delay;
		return this;
	}

	/**
	 * Optional (Default: 65536) maximum message size in bytes. Allowed values: 1024-65536 and -1 (for unlimited size).
	 */
	public CreateQueueCmd maxsize(int maxsize) {
		this.maxsize = maxsize;
		return this;
	}

	/**
	 * @return 1
	 */
	@Override
	protected Integer exec(RedisCommands<String, String> redisCommands) {
		Validator.create()
			.assertValidQname(qname)
			.assertValidVt(vt)
			.assertValidDelay(delay)
			.assertValidMaxSize(maxsize);

		List<String> times = redisCommands.time();

		redisCommands.multi();

		String key = config.redisNs() + qname + Q;

		redisCommands.hsetnx(key, "vt", String.valueOf(vt));
		redisCommands.hsetnx(key, "delay", String.valueOf(delay));
		redisCommands.hsetnx(key, "maxsize", String.valueOf(maxsize));
		redisCommands.hsetnx(key, "created", times.get(0));
		redisCommands.hsetnx(key, "modified", times.get(0));

		TransactionResult transactionResult = redisCommands.exec();

		@SuppressWarnings({"rawtypes", "unchecked"})
		List<Boolean> results = (List)transactionResult.stream().toList();

		int createdCount = results.stream().anyMatch(result -> !result) ? 0 : 1;

		if (createdCount == 0) {
			throw new RedisSMQException("Queue already exists: " + qname);
		}

		redisCommands.sadd(config.redisNs() + QUEUES, qname);

		return createdCount;
	}
}
