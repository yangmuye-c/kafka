/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.Utils;

/**
 * An internal class that implements a cache used for sticky partitioning behavior. The cache tracks the current sticky
 * partition for any given topic. This class should not be used externally.
 * kafkaproducer发送数据并不是一个一个消息发送，而是取决于两个producer端参数。
 * 一个是linger.ms，默认是0ms，当达到这个时间后，kafkaproducer就会立刻向broker发送数据。
 * 另一个参数是batch.size，默认是16kb，当产生的消息数达到这个大小后，就会立即向broker发送数据。
 * 按照这个设计，从直观上思考，肯定是希望每次都尽可能填满一个batch再发送到一个分区。
 * 但实际决定batch如何形成的一个因素是分区策略（partitionerstrategy）。
 * 在Kafka2.4版本之前，在producer发送数据默认的分区策略是轮询策略（没指定keyd的情况。如果多条消息不是被发送到相同的分区，它们就不能被放入到一个batch中。
 * 所以如果使用默认的轮询partition策略，可能会造成一个大的batch被轮询成多个小的batch的情况。
 * 鉴于此，kafka2.4的时候推出一种新的分区策略，即StickyPartitioning Strategy，
 * StickyPartitioning Strategy会随机地选择另一个分区并会尽可能地坚持使用该分区——即所谓的粘住这个分区。
 * 鉴于小batch可能导致延时增加，之前对于无Key消息的分区策略效率很低。社区于2.4版本引入了黏性分区策略（StickyPartitioning Strategy）。
 * 该策略是一种全新的策略，能够显著地降低给消息指定分区过程中的延时。使用StickyPartitioner有助于改进消息批处理，减少延迟，并减少broker的负载。
 */
public class StickyPartitionCache {
    private final ConcurrentMap<String, Integer> indexCache;
    public StickyPartitionCache() {
        this.indexCache = new ConcurrentHashMap<>();
    }

    public int partition(String topic, Cluster cluster) {
        //从缓存取，取到则返回
        Integer part = indexCache.get(topic);
        if (part == null) {
            //取不到则计算
            return nextPartition(topic, cluster, -1);
        }
        return part;
    }

    /**
     * 未指定分区切未指定key的情况下计算分区，采用粘性分区策略,2.4版本之前默认是轮训，2.4之后默认是粘性分区
     * @param topic
     * @param cluster
     * @param prevPartition
     * @return
     */
    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
        //获取分区个数
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        //indexCache 保存的是 topic -> 下一条消息要发送的分区，这里查到是null
        Integer oldPart = indexCache.get(topic);

        Integer newPart = oldPart;
        // Check that the current sticky partition for the topic is either not set or that the partition that
        // 检查主题的当前粘性分区是否未设置，或者该分区是否已设置
        // triggered the new batch matches the sticky partition that needs to be changed.
        // 触发新批处理匹配需要更改的粘接分区。
        //这里oldPart == null 条件一定成立
        if (oldPart == null || oldPart == prevPartition) {
            //获取可用的Partition
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            //没有可用分区的情况
            if (availablePartitions.size() < 1) {
                //获取一个随机数，然后对总分区数取余
                Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                newPart = random % partitions.size();
            } else if (availablePartitions.size() == 1) {
                //只有一个可用分区的情况，就只能选择这个分区
                newPart = availablePartitions.get(0).partition();
            } else {
                //有多个可用分区
                while (newPart == null || newPart.equals(oldPart)) {
                    //获取随机数，然后对可用分区取余
                    Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                    newPart = availablePartitions.get(random % availablePartitions.size()).partition();
                }
            }
            // Only change the sticky partition if it is null or prevPartition matches the current sticky partition.
            // 只有当它为null或prevPartition与当前的sticky分区匹配时，才会更改sticky分区。
            //将计算的分区缓存入indexCache
            if (oldPart == null) {
                indexCache.putIfAbsent(topic, newPart);
            } else {
                indexCache.replace(topic, prevPartition, newPart);
            }
            return indexCache.get(topic);
        }
        return indexCache.get(topic);
    }

}