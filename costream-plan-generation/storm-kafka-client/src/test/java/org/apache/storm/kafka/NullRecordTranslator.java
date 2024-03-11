/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;

public class NullRecordTranslator<K, V> implements RecordTranslator<K, V> {

    @Override
    public List<Object> apply(ConsumerRecord<K, V> record) {
        return null;

    }

    @Override
    public Fields getFieldsFor(String stream) {
        return new Fields("topic", "key", "value");
    }
}
