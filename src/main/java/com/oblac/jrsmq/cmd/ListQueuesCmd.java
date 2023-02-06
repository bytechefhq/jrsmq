package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.RedisSMQConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.Set;
import java.util.function.Supplier;

import static com.oblac.jrsmq.Values.QUEUES;

/**
 * List all queues.
 */
public class ListQueuesCmd extends BaseQueueCmd<Set<String>> {

	public ListQueuesCmd(RedisSMQConfig config, Supplier<RedisClient> jedisSupplier) {
		super(config, jedisSupplier);
	}

	/**
	 * @return collection of all queue names.
	 */
	@Override
	protected Set<String> exec(RedisCommands<String, String> redisCommands) {
		return redisCommands.smembers(config.redisNs() + QUEUES);
	}
}
