package com.oblac.jrsmq.cmd;

import com.oblac.jrsmq.QueueDef;
import com.oblac.jrsmq.RedisSMQConfig;
import com.oblac.jrsmq.Validator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.function.Supplier;

/**
 * Change the visibility timer of a single message. The time when the message
 * will be visible again is calculated from the current time (now) + vt.
 */
public class ChangeMessageVisibilityCmd extends BaseQueueCmd<Integer> {

	private final String changeMessageVisibilitySha1;
	private String qname;
	private String id;
	private int vt;

	public ChangeMessageVisibilityCmd(RedisSMQConfig config, Supplier<RedisClient> jedisSupplier, String changeMessageVisibilitySha1) {
		super(config, jedisSupplier);
		this.changeMessageVisibilitySha1 = changeMessageVisibilitySha1;
	}

	/**
	 * The Queue name.
	 */
	public ChangeMessageVisibilityCmd qname(String qname) {
		this.qname = qname;
		return this;
	}

	/**
	 * The message id.
	 */
	public ChangeMessageVisibilityCmd id(String id) {
		this.id = id;
		return this;
	}

	/**
	 * The length of time, in seconds, that this message will not be visible. Allowed values: 0-9999999.
	 */
	public ChangeMessageVisibilityCmd vt(int vt) {
		this.vt = vt;
		return this;
	}

	/**
	 * @return 1 if successful, 0 if the message was not found.
	 */
	@Override
	protected Integer exec(RedisCommands<String, String> redisCommands) {
		Validator.create()
			.assertValidQname(qname)
			.assertValidVt(vt)
			.assertValidId(id);

		QueueDef q = getQueue(redisCommands, qname, false);

		Long foo = redisCommands.evalsha(changeMessageVisibilitySha1, ScriptOutputType.INTEGER, config.redisNs() + qname, id, String.valueOf(q.ts() + vt * 1000));

		return foo.intValue();
	}
}
