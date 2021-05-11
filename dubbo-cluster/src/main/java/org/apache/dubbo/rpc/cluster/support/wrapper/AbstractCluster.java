/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.support.wrapper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.interceptor.ClusterInterceptor;
import org.apache.dubbo.rpc.cluster.support.AbstractClusterInvoker;

import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.REFERENCE_INTERCEPTOR_KEY;

//基于模板模式实现
//AbstractCluster实现了基于拦截器的在业务执行前后添加执行业务逻辑的功能
public abstract class AbstractCluster implements Cluster {

    private <T> Invoker<T> buildClusterInterceptors(AbstractClusterInvoker<T> clusterInvoker, String key) {
        AbstractClusterInvoker<T> last = clusterInvoker;

        //获取所有的拦截器的列表
        List<ClusterInterceptor> interceptors = ExtensionLoader.getExtensionLoader(ClusterInterceptor.class)
                .getActivateExtension(clusterInvoker.getUrl(), key);

        //使用InterceptorInvokerNode把ClusterInterceptor封装成一个责任链
        //
        if (!interceptors.isEmpty()) {
            for (int i = interceptors.size() - 1; i >= 0; i--) {
                final ClusterInterceptor interceptor = interceptors.get(i);
                final AbstractClusterInvoker<T> next = last;
                //新建一个拦截器链的节点，并把next节点设置上去，InterceptorInvokerNode是一个单向的责任链
                last = new InterceptorInvokerNode<>(clusterInvoker, interceptor, next);
            }
        }
        return last;
    }

    @Override
    public <T> Invoker<T> join(Directory<T> directory) throws RpcException {
        // 使用子类doJoin来真正生成Invoker
        // 并且使用拦截器的方式进行一层封装
        return buildClusterInterceptors(doJoin(directory), directory.getUrl().getParameter(REFERENCE_INTERCEPTOR_KEY));
    }

    /**
     * 具体生成一个新的invoker的业务逻辑由doJoin方法完成
     *
     * @param directory
     * @param <T>
     * @return
     * @throws RpcException
     */
    protected abstract <T> AbstractClusterInvoker<T> doJoin(Directory<T> directory) throws RpcException;

    /**
     * 单向责任链，用于支持拦截器来执行前置和后置业务逻辑，每个实例表示责任链中的一个节点
     *
     * @param <T>
     */
    protected class InterceptorInvokerNode<T> extends AbstractClusterInvoker<T> {

        private AbstractClusterInvoker<T> clusterInvoker;
        //拦截器
        private ClusterInterceptor interceptor;
        //下一个ClusterInvoker实例
        private AbstractClusterInvoker<T> next;

        public InterceptorInvokerNode(AbstractClusterInvoker<T> clusterInvoker,
                                      ClusterInterceptor interceptor,
                                      AbstractClusterInvoker<T> next) {
            this.clusterInvoker = clusterInvoker;
            this.interceptor = interceptor;
            this.next = next;
        }

        @Override
        public Class<T> getInterface() {
            return clusterInvoker.getInterface();
        }

        @Override
        public URL getUrl() {
            return clusterInvoker.getUrl();
        }

        @Override
        public boolean isAvailable() {
            return clusterInvoker.isAvailable();
        }

        @Override
        public Result invoke(Invocation invocation) throws RpcException {
            Result asyncResult;
            try {
                //执行前置拦截器方法
                interceptor.before(next, invocation);
                asyncResult = interceptor.intercept(next, invocation);
            } catch (Exception e) {
                // onError callback
                // 处理异常
                if (interceptor instanceof ClusterInterceptor.Listener) {
                    ClusterInterceptor.Listener listener = (ClusterInterceptor.Listener) interceptor;
                    listener.onError(e, clusterInvoker, invocation);
                }
                throw e;
            } finally {
                //执行后置拦截器的业务逻辑
                interceptor.after(next, invocation);
            }
            return asyncResult.whenCompleteWithContext((r, t) -> {
                // onResponse callback
                if (interceptor instanceof ClusterInterceptor.Listener) {
                    ClusterInterceptor.Listener listener = (ClusterInterceptor.Listener) interceptor;
                    if (t == null) {
                        listener.onMessage(r, clusterInvoker, invocation);
                    } else {
                        listener.onError(t, clusterInvoker, invocation);
                    }
                }
            });
        }

        @Override
        public void destroy() {
            clusterInvoker.destroy();
        }

        @Override
        public String toString() {
            return clusterInvoker.toString();
        }

        @Override
        protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
            // The only purpose is to build a interceptor chain, so the cluster related logic doesn't matter.
            return null;
        }
    }
}
