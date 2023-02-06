package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.QueueDef;
import com.oblac.jrsmq.QueueMessage;
import com.oblac.jrsmq.RedisSMQConfig;
import com.oblac.jrsmq.Validator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;
import java.util.function.Supplier;

import static com.oblac.jrsmq.Values.UNSET_VALUE;

/**
 * Receive the next message from the queue.
 */
public class ReceiveMessageCmd extends BaseQueueCmd<QueueMessage> {

	private final String receiveMessageSha1;
	private String name;
	private int vt = UNSET_VALUE;

	public ReceiveMessageCmd(RedisSMQConfig config, Supplier<RedisClient> jedisSupplier, String receiveMessageSha1) {
		super(config, jedisSupplier);
		this.receiveMessageSha1 = receiveMessageSha1;
	}

	/**
	 * The Queue name.
	 */
	public ReceiveMessageCmd qname(String name) {
		this.name = name;
		return this;
	}

	/**
	 * Optional (Default: queue settings) length of time, in seconds, that the
	 * received message will be invisible to others. Allowed values: 0-9999999 (around 115 days)
	 */
	public ReceiveMessageCmd vt(int vt) {
		this.vt = vt;
		return this;
	}

	/**
	 * @return {@link QueueMessage} or {@code null} if message is not there.
	 */
	@Override
	protected QueueMessage exec(RedisCommands<String, String> redisCommands) {
		Validator.create()
			.assertValidQname(name);

		QueueDef q = getQueue(redisCommands, name, false);

		int vt = this.vt;
		if (vt == UNSET_VALUE) {
			vt = q.vt();
		}
		Validator.create().assertValidVt(vt);

		List<?> result = redisCommands.evalsha(
			receiveMessageSha1, ScriptOutputType.MULTI, config.redisNs() + name, String.valueOf(q.ts()), String.valueOf(q.ts() + vt * 1000));

		return createQueueMessage(result);
	}
}
