from kafka.admin import KafkaAdminClient, NewTopic


# 创建topic

def create_topic(bootstrap_server: str, topic_name: str,
                 partitions_num: int):
    '''
        #bootstrap服务器提供的是连接和通点选集群中的一台就可以（不用把集群里面所有机器都链接）
        bootstrap_servers = '127.0.0.1:9092'
       	#要检查的 topic 名称
       	topic_name = 'xxxx'
       	#分区数量
       	partitions_count = 16
    '''
    # bootstrap_server = settings.KAFKA_SERVERS[0]
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_server)

    # 检查topic是否存在
    topics = admin_client.list_topics()
    if topic_name not in topics:
        new_topic = NewTopic(
            name=topic_name,
            # num_partitions=partitions_num,
            num_partitions=partitions_num,
            replication_factor=1,  # 副本因子，应小于或等于Kafka集群中的broker数量
        )
        admin_client.create_topics([new_topic])
        print(f'Topic {topic_name} created with {partitions_num} partitions.')

    admin_client.close()


if __name__ == '__main__':
    create_topic(bootstrap_server='127.0.0.1:9092', topic_name='demo-1', partitions_num=1)
