package yanbinwa.iCollection.service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import yanbinwa.common.kafka.message.KafkaMessage;
import yanbinwa.common.kafka.producer.IKafkaProducer;
import yanbinwa.common.kafka.producer.IKafkaProducerImpl;
import yanbinwa.common.orchestrationClient.OrchestartionCallBack;
import yanbinwa.common.orchestrationClient.OrchestrationClient;
import yanbinwa.common.orchestrationClient.OrchestrationClientImpl;
import yanbinwa.common.orchestrationClient.OrchestrationServiceState;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.iCollection.exception.ServiceUnavailableException;

@Service("collectionService")
@EnableAutoConfiguration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "serviceProperties")
public class CollectionServiceImpl implements CollectionService
{

    private static final Logger logger = Logger.getLogger(CollectionServiceImpl.class);
    
    Map<String, String> serviceDataProperties;
    Map<String, String> zNodeInfoProperties;
    Map<String, String> kafkaProducerProperties;
    
    public void setServiceDataProperties(Map<String, String> properties)
    {
        this.serviceDataProperties = properties;
    }
    
    public Map<String, String> getServiceDataProperties()
    {
        return this.serviceDataProperties;
    }
    
    public void setZNodeInfoProperties(Map<String, String> properties)
    {
        this.zNodeInfoProperties = properties;
    }
    
    public Map<String, String> getZNodeInfoProperties()
    {
        return this.zNodeInfoProperties;
    }
    
    public void setKafkaProducerProperties(Map<String, String> properties)
    {
        this.kafkaProducerProperties = properties;
    }
    
    public Map<String, String> getKafkaProducerProperties()
    {
        return this.kafkaProducerProperties;
    }
    
    ZNodeServiceData serviceData = null;
        
    OrchestrationClient client = null;
    
    IKafkaProducer kafkaProducer = null;
    
    boolean isRunning = false;
    
    OrchestrationWatcher watcher = new OrchestrationWatcher();
        
    @Override
    public void afterPropertiesSet() throws Exception
    {
        String zookeeperHostIp = zNodeInfoProperties.get(OrchestrationClient.ZOOKEEPER_HOSTPORT_KEY);
        if(zookeeperHostIp == null)
        {
            logger.error("Zookeeper host and port should not be null");
            return;
        }
        
        String serviceName = serviceDataProperties.get(CollectionService.SERVICE_SERVICENAME);
        String ip = serviceDataProperties.get(CollectionService.SERVICE_IP);
        String portStr = serviceDataProperties.get(CollectionService.SERVICE_PORT);
        int port = Integer.parseInt(portStr);
        String rootUrl = serviceDataProperties.get(CollectionService.SERVICE_ROOTURL);
        serviceData = new ZNodeServiceData(ip, serviceName, port, rootUrl);
        
        client = new OrchestrationClientImpl(serviceData, watcher, zookeeperHostIp, zNodeInfoProperties);
        kafkaProducer = new IKafkaProducerImpl(kafkaProducerProperties, new HashSet<String>(), "kafkaProducer");
        start();
    }

    @Override
    public void start()
    {
        if(!isRunning)
        {
            logger.info("Start collection service ...");
            client.start();
            isRunning = true;
        }
        else
        {
            logger.info("Collection service has already started ...");
        }
    }

    @Override
    public void stop()
    {
        if(isRunning)
        {
            logger.info("Stop collection service ...");
            client.stop();
            isRunning = false;
        }
        else
        {
            logger.info("Collection service has already stopped ...");
        }
    }

    @Override
    public String getServiceName() throws ServiceUnavailableException
    {
        if(!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return serviceData.getServiceName();
    }

    @Override
    public boolean isServiceReady() throws ServiceUnavailableException
    {
        if(!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return client.isReady();
    }

    @Override
    public String getServiceDependence() throws ServiceUnavailableException
    {
        if(!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return client.getDepData().toString();
    }

    @Override
    public void startManageService()
    {
        if(!isRunning)
        {
            start();
        }
    }

    @Override
    public void stopManageService()
    {
        if(isRunning)
        {
            stop();
        }
    }
    
    class OrchestrationWatcher implements OrchestartionCallBack
    {
        
        OrchestrationServiceState curState = OrchestrationServiceState.NOTREADY;
        Thread sendMessageThread = null;
        boolean isSendMessageThreadRun = false;
        
        private void sendMessageOneByOne()
        {
            while(isSendMessageThreadRun)
            {
                KafkaMessage msg = new KafkaMessage(1, "HelloWord");
                logger.info("Send a msg: " + msg);
                kafkaProducer.sendMessage(msg);
                try
                {
                    Thread.sleep(5000);
                } 
                catch (InterruptedException e)
                {
                    if(!isSendMessageThreadRun)
                    {
                        logger.info("Stop send message thread");
                    }
                    else
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
        
        /**
         * 如果Ready，就根据dependence开始计算当前的topic list，从而对producer进行update，之后就开启producer，同时定期写入数据
         * 如果DependChange，就根据dependence更新当前的topic list
         * 如果UnReady，就停止producer，但是不会丢掉未发出的Msg，一旦Ready，会定期继续发数据
         */
        @Override
        public void handleServiceStateChange(OrchestrationServiceState state)
        {
            logger.info("Service state is: " + state);
            //由Unready -> ready
            if (state == OrchestrationServiceState.READY && curState == OrchestrationServiceState.NOTREADY)
            {
                logger.info("The service is started");
                Set<String> topic = new HashSet<String>();
                topic.add("wyb");
                kafkaProducer.updateTopic(topic);
                kafkaProducer.start();
                sendMessageThread = new Thread(new Runnable(){

                    @Override
                    public void run()
                    {
                        sendMessageOneByOne();
                    }
                    
                });
                isSendMessageThreadRun = true;
                sendMessageThread.start();
                curState = state;
            }
            else if(state == OrchestrationServiceState.NOTREADY && curState == OrchestrationServiceState.READY)
            {
                logger.info("The service is stopped");
                isSendMessageThreadRun = false;
                sendMessageThread.interrupt();
                kafkaProducer.stop();
                curState = state;
            }
            else if(state == OrchestrationServiceState.DEPCHANGE)
            {
                Set<String> topic = new HashSet<String>();
                topic.add("wyb");
                kafkaProducer.updateTopic(topic);
                logger.info("The dependence is changed");
            }
        }
    }
}
