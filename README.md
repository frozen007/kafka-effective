# kafka-effective
use apache kafka consumer and producer more effectively

## 为什么做这个项目
之前一直在项目中使用kafka原生的Consumer和Producer API，但总是觉得有点不方便：
* 初始化加载的时候比较繁琐
* 使用Consumer的高级API无法真正实现批量处理(低级API超复杂又不会用T_T)
* 每次部署一个新topic都会进行很多重复的配置工作

后来读了flume中有关与kafka集成的代码(org.apache.flume.source.kafka.KafkaSource)，觉得其中使用kafka Consumer的方式比较合理，可以实现消息的批处理，所以就参考并封装了一下，变得更加通用，形成了这个项目。目前已经被应用于我们的线上服务中，用来处理每天大量的用户数据流和Feed数据流。


## 用法
### BatchConsumer
```
    <!--初始化BatchConsumer，会自动启动一个线程-->
    <bean id="kafkaBatchConsumer" class="com.myz.base.kafka.KafkaBatchConsumer" init-method="start">
        <property name="zkConnect" value="${kafka.queue.zookeeper_connect}"/>
        <property name="topic" value="${topicName}"/>
        <property name="groupId" value="$group_id}"/>
        <property name="consumerId" value="${consumer_id}"/>
        <property name="autoCommit" value="false"/>
        <property name="consumerTimeout" value="10"/>
        <property name="messageConsumer" ref="messageConsumer"/>
    </bean>

    <!--实现一个具体的消息处理类，里面可以加入自己的处理逻辑-->
    <bean id="messageConsumer" class="xxxx.queue.FeedMessageConsumer">
    </bean>

```

### Producer
```
    <!--初始化一个KafkaProducerFactory-->
    <bean id="producerFactory" class="com.asiainno.base.kafka.StringBasedKafkaProducerFactory" init-method="init">
        <property name="props">
            <props>
                <prop key="metadata.broker.list">${metadata_broker_list}</prop>
                <prop key="serializer.class">kafka.serializer.StringEncoder</prop>
                <!--需要提供一个自定义的Partitioner-->
                <prop key="partitioner.class">xxxxx.MessagePartitioner</prop>
                <prop key="request.required.acks">1</prop>
            </props>
        </property>
    </bean>

    <!--生成一个指定topic的producer-->
    <bean id="topic1Producer" factory-bean="producerFactory" factory-method="getProducer">
        <property name="queueTopic" value="${topicName1}"/>
    </bean>

    <bean id="topic2Producer" factory-bean="producerFactory" factory-method="getProducer">
        <property name="queueTopic" value="${topicName2}"/>
    </bean>
```