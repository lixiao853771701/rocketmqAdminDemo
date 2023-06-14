package com.xiaoli.demo.mq.rocketmq;

import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lixiao
 * @date 2023/5/18 10:53
 * @Description :
 */
@Slf4j
@RestController
@RequestMapping("rocketCunstomMenage")
public class CreateTopicController {

    @RequestMapping("/createTopic")
    public String createTopic(TopicConfigDto topicConfigDto){

        //创建topic
        String topicName = topicConfigDto.getTopicName();
        if (StringUtils.isNotEmpty(topicName)) {
            DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
            defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
            try {
                TopicConfig topicConfig = new TopicConfig();
                topicConfig.setReadQueueNums(8);
                topicConfig.setWriteQueueNums(8);
                topicConfig.setTopicName(topicName);

                CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
                requestHeader.setTopic(topicConfig.getTopicName());
                requestHeader.setDefaultTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);
                requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
                requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
                requestHeader.setPerm(topicConfig.getPerm());
                requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
                requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
                requestHeader.setOrder(topicConfig.isOrder());
                defaultMQAdminExt.setNamesrvAddr(topicConfigDto.getNameSrvAddr());
                defaultMQAdminExt.start();

                Set<String> masterSet =
                        CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, topicConfigDto.getClusterName());
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                    System.out.printf("create topic to %s success.%n", addr);
                }
                System.out.printf("%s", topicConfig);
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                defaultMQAdminExt.shutdown();
            }
        }else {
            log.info("topicname is empty");
        }

        //创建group 可以成功，生产者和消费者都可以发收消息 ，但是创建完成后找不到group，需要生成者消费者发收消息后可以看到
        String groupName = topicConfigDto.getGroupName();
        if (StringUtils.isNotEmpty(groupName)) {
            DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
            mqAdminExt.setNamesrvAddr(topicConfigDto.getNameSrvAddr());
            SubscriptionGroupConfig config = new SubscriptionGroupConfig();
            config.setGroupName(groupName);
            try {
                mqAdminExt.start();
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt,
                        topicConfigDto.getClusterName());
                for (String addr : masterSet) {
                    try {
                        mqAdminExt.createAndUpdateSubscriptionGroupConfig(addr, config);
                        log.info(String.format("create subscription group %s to %s success.\n", topicConfigDto.getClusterName(),
                                addr));
                    } catch (Exception e) {
                        e.printStackTrace();
                        Thread.sleep(1000 * 1);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                mqAdminExt.shutdown();
            }
        }else {
            log.info("groupname is empty");
        }


        return "success";

    }

}
