package org.apache.rocketmq.tools.command;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public abstract class AbstractCommand {

    private DefaultMQAdminExt defaultMQAdminExt;

    public DefaultMQAdminExt getMQAdminExt(RPCHook rpcHook) throws SubCommandException {
        if(defaultMQAdminExt!=null){
            return defaultMQAdminExt;
        }
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        try {
            defaultMQAdminExt.start();
            this.defaultMQAdminExt =  defaultMQAdminExt;
            return this.defaultMQAdminExt;
        } catch (MQClientException e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        }
    }
}
