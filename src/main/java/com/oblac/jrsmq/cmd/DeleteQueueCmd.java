package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.RedisSMQConfig;
import com.oblac.jrsmq.RedisSMQException;
import com.oblac.jrsmq.Validator;
import com.oblac.jrsmq.Values;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;
import java.util.function.Supplier;

import static com.oblac.jrsmq.Util.toInt;
import static com.oblac.jrsmq.Values.QUEUES;

/**
 * Delete a queue and all messages.
 */
public class DeleteQueueCmd extends BaseQueueCmd<Integer> {

	private String qname;

	public DeleteQueueCmd(RedisSMQConfig config, Supplier<RedisClient> jedisSupplier) {
		super(config, jedisSupplier);
	}

	/**
	 * The Queue name.
	 */
	public DeleteQueueCmd qname(String qname) {
		this.qname = qname;
		return this;
	}

	/**
	 * @return 1
	 */
	@Override
	protected Integer exec(RedisCommands<String, String> redisCommands) {
		Validator.create()
			.assertValidQname(qname);

		String key = config.redisNs() + qname;

		redisCommands.multi();

		redisCommands.del(key + Values.Q);
		redisCommands.del(key);
		redisCommands.srem(config.redisNs() + QUEUES, qname);

		TransactionResult transactionResult = redisCommands.exec();

		List<?> result = transactionResult.stream().toList();

		if (toInt(result, 0) == 0) {
			throw new RedisSMQException("Queue not found: " + qname);
		}

		return 1;
	}


}
