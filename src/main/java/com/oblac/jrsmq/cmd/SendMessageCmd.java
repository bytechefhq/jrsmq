package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.QueueDef;
import com.oblac.jrsmq.RedisSMQConfig;
import com.oblac.jrsmq.Validator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.function.Supplier;

import static com.oblac.jrsmq.Values.Q;

/**
 * Send a new message.
 */
public class SendMessageCmd extends BaseQueueCmd<String> {

	private String qname;
	private String message;
	private int delay;

	public SendMessageCmd(RedisSMQConfig config, Supplier<RedisClient> jedisSupplier) {
		super(config, jedisSupplier);
	}

	/**
	 * The Queue name.
	 */
	public SendMessageCmd qname(String qname) {
		this.qname = qname;
		return this;
	}

	/**
	 * The message's contents.
	 */
	public SendMessageCmd message(String message) {
		this.message = message;
		return this;
	}

	/**
	 * Optional (Default: queue settings) time in seconds that the delivery of
	 * the message will be delayed. Allowed values: 0-9999999 (around 115 days)
	 */
	public SendMessageCmd delay(int delay) {
		this.delay = delay;
		return this;
	}

	/**
	 * @return The internal message id.
	 */
	@Override
	protected String exec(RedisCommands<String, String> redisCommands) {
		QueueDef q = getQueue(redisCommands, qname, true);

		Validator.create()
			.assertValidQname(qname)
			.assertValidDelay(delay)
			.assertValidMessage(q, message);

		redisCommands.multi();

		String key = config.redisNs() + qname + Q;

		redisCommands.zadd(config.redisNs() + qname, q.ts() + delay * 1000, q.uid());
		redisCommands.hset(key, q.uid(), message);
		redisCommands.hincrby(key, "totalsent", 1);

		redisCommands.exec();

		return q.uid();
	}
}
