package yanbinwa.iCollection.service;

import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

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
    
    Map<String, String> serviceDataProperites;
    Map<String, String> zNodeInfoProperites;
    
    public void setServiceDataProperites(Map<String, String> properties)
    {
        this.serviceDataProperites = properties;
    }
    
    public Map<String, String> getServiceDataProperites()
    {
        return this.serviceDataProperites;
    }
    
    public void setZNodeInfoProperites(Map<String, String> properties)
    {
        this.zNodeInfoProperites = properties;
    }
    
    public Map<String, String> getZNodeInfoProperites()
    {
        return this.zNodeInfoProperites;
    }
    
    ZNodeServiceData serviceData = null;
        
    OrchestrationClient client = null;
    
    boolean isRunning = false;
    
    OrchestrationWatcher watcher = new OrchestrationWatcher();
        
    @Override
    public void afterPropertiesSet() throws Exception
    {
        String zookeeperHostIp = zNodeInfoProperites.get(OrchestrationClient.ZOOKEEPER_HOSTPORT_KEY);
        if(zookeeperHostIp == null)
        {
            logger.error("Zookeeper host and port should not be null");
            return;
        }
        
        String serviceName = serviceDataProperites.get(CollectionService.SERVICE_SERVICENAME);
        String ip = serviceDataProperites.get(CollectionService.SERVICE_IP);
        String portStr = serviceDataProperites.get(CollectionService.SERVICE_PORT);
        int port = Integer.parseInt(portStr);
        String rootUrl = serviceDataProperites.get(CollectionService.SERVICE_ROOTURL);
        serviceData = new ZNodeServiceData(ip, serviceName, port, rootUrl);
        
        client = new OrchestrationClientImpl(serviceData, watcher, zookeeperHostIp, zNodeInfoProperites);
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

        @Override
        public void handleServiceStateChange(OrchestrationServiceState state)
        {
            logger.info("Service state is: " + state);
        }
        
    }
    
}
