package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.RedisSMQConfig;
import com.oblac.jrsmq.Validator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;
import java.util.function.Supplier;

import static com.oblac.jrsmq.Util.toInt;
import static com.oblac.jrsmq.Values.Q;

/**
 * Delete a message.
 */
public class DeleteMessageCmd extends BaseQueueCmd<Integer> {

	private String name;
	private String id;

	public DeleteMessageCmd(RedisSMQConfig config, Supplier<RedisClient> jedisSupplier) {
		super(config, jedisSupplier);
	}

	/**
	 * The Queue name.
	 */
	public DeleteMessageCmd qname(String name) {
		this.name = name;
		return this;
	}

	/**
	 * Message id to delete.
	 */
	public DeleteMessageCmd id(String id) {
		this.id = id;
		return this;
	}

	/**
	 * 1 if successful, 0 if the message was not found.
	 */
	@Override
	protected Integer exec(RedisCommands<String, String> redisCommands) {
		Validator.create()
			.assertValidQname(name)
			.assertValidId(id);

		String key = config.redisNs() + name;

		redisCommands.multi();

		redisCommands.zrem(key, id);
		redisCommands.hdel(key + Q, id, id + ":rc", id + ":fr");

		TransactionResult transactionResult = redisCommands.exec();

		List<?> result = transactionResult.stream().toList();

		if (toInt(result, 0) == 1 && toInt(result, 1) > 0) {
			return 1;
		}

		return 0;
	}
}