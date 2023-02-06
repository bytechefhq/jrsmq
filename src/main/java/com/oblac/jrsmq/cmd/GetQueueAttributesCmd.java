package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.QueueAttributes;
import com.oblac.jrsmq.RedisSMQConfig;
import com.oblac.jrsmq.RedisSMQException;
import com.oblac.jrsmq.Util;
import com.oblac.jrsmq.Validator;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;
import java.util.function.Supplier;

import static com.oblac.jrsmq.Values.Q;

/**
 * Get queue attributes, counter and stats.
 */
public class GetQueueAttributesCmd extends BaseQueueCmd<QueueAttributes> {

	private String qname;

	public GetQueueAttributesCmd(RedisSMQConfig config, Supplier<RedisClient> jedisSupplier) {
		super(config, jedisSupplier);
	}

	/**
	 * The Queue name.
	 */
	public GetQueueAttributesCmd qname(String qname) {
		this.qname = qname;
		return this;
	}

	/**
	 * @return {@link QueueAttributes}
	 */
	@Override
	protected QueueAttributes exec(RedisCommands<String, String> redisCommands) {
		Validator.create().assertValidQname(qname);

		List<String> times = redisCommands.time();

		String key = config.redisNs() + qname;

		redisCommands.multi();

		redisCommands.hmget(key + Q, "vt", "delay", "maxsize", "totalrecv", "totalsent", "created", "modified");
		redisCommands.zcard(key);
		redisCommands.zcount(key, times.get(0) + "000", "+inf");

		TransactionResult transactionResult = redisCommands.exec();

		List<?> results = transactionResult.stream().toList();

		if (results.get(0) == null) {
			throw new RedisSMQException("Queue not found: " + qname);
		}

		List<KeyValue<String, String>> rec0 = (List<KeyValue<String, String>>) results.get(0);

		QueueAttributes qa = new QueueAttributes(
			Integer.parseInt(getValue(rec0.get(0))),
			Integer.parseInt(getValue(rec0.get(1))),
			Integer.parseInt(getValue(rec0.get(2))),
			Util.safeParseLong(getValue(rec0.get(3))),
			Util.safeParseLong(getValue(rec0.get(4))),
			Long.parseLong(getValue(rec0.get(5))),
			Long.parseLong(getValue(rec0.get(6))),
			(Long) results.get(1),
			(Long) results.get(2)
		);

		return qa;
	}

	private String getValue(KeyValue<String, String> keyValue) {
		if (keyValue.hasValue()) {
			return keyValue.getValue();
		}

		return null;
	}
}
