package com.xiaoli.demo.mq.rocketmq;

import lombok.Data;

/**
 * @author lixiao
 * @projectName rocketmqAdminDemo
 * @date 2023/5/19 9:49
 * @Description :
 */
@Data
public class TopicConfigDto {

    private String topicName;

    private String nameSrvAddr;

    private String groupName;

    private String clusterName = "DefaultCluster";

}
