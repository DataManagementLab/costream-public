/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.streams;

import java.util.HashMap;

import org.apache.storm.topology.IRichSpout;
import org.apache.storm.utils.Utils;

/**
 * A spout node wraps an {@link IRichSpout}.
 */
class SpoutNode extends Node {
    private final IRichSpout spout;
    private final HashMap<String, Object> description;

    SpoutNode(IRichSpout spout, HashMap<String, Object> description) {
        super(Utils.DEFAULT_STREAM_ID, getOutputFields(spout, Utils.DEFAULT_STREAM_ID));
        this.description = description;
        if (outputFields.size() == 0) {
            throw new IllegalArgumentException("Spout " + spout + " does not declare any fields"
                    + "for the stream '" + Utils.DEFAULT_STREAM_ID + "'");
        }
        this.spout = spout;
    }

    SpoutNode(IRichSpout spout) {
        this(spout, new HashMap<>());

    }

    IRichSpout getSpout() {
        return spout;
    }

    HashMap<String, Object> getDescription() {
        return this.description;
    }

    @Override
    void addOutputStream(String streamId) {
        throw new UnsupportedOperationException("Cannot add output streams to a spout node");
    }

}
