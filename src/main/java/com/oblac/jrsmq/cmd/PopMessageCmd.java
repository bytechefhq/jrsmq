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

/**
 * Receive the next message from the queue and delete it.
 * <br>
 * Important: This method deletes the message it receives right away.
 * There is no way to receive the message again if something goes wrong while working on the message.
 */
public class PopMessageCmd extends BaseQueueCmd<QueueMessage> {

	private final String popMessageSha1;
	private String qname;

	public PopMessageCmd(RedisSMQConfig config, Supplier<RedisClient> jedisSupplier, String popMessageSha1) {
		super(config, jedisSupplier);
		this.popMessageSha1 = popMessageSha1;
	}

	/**
	 * The Queue name.
	 */
	public PopMessageCmd qname(String qname) {
		this.qname = qname;
		return this;
	}

	/**
	 * @return {@link QueueMessage} or {@code null} if no message is there.
	 */
	@Override
	protected QueueMessage exec(RedisCommands<String, String> redisCommands) {
		Validator.create().assertValidQname(qname);

		QueueDef q = getQueue(redisCommands, qname, false);

		List<?> result = redisCommands.evalsha(popMessageSha1, ScriptOutputType.MULTI, config.redisNs() + qname, String.valueOf(q.ts()));

		return createQueueMessage(result);
	}
}
