package yanbinwa.iCollection.service;

import org.springframework.beans.factory.InitializingBean;

import yanbinwa.common.iInterface.ServiceLifeCycle;
import yanbinwa.iCollection.exception.ServiceUnavailableException;

public interface CollectionService extends InitializingBean, ServiceLifeCycle
{
    
    public static final String SERVICE_IP = "ip";
    public static final String SERVICE_SERVICENAME = "serviceName";
    public static final String SERVICE_PORT = "port";
    public static final String SERVICE_ROOTURL = "rootUrl";
    
    String getServiceName() throws ServiceUnavailableException;
    
    boolean isServiceReady() throws ServiceUnavailableException;
    
    String getServiceDependence() throws ServiceUnavailableException;

    void startManageService();

    void stopManageService();
}
