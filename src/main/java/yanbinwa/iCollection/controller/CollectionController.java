package yanbinwa.iCollection.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import yanbinwa.iCollection.exception.ServiceUnavailableException;
import yanbinwa.iCollection.service.CollectionService;

@RestController
@RequestMapping("/collection")
public class CollectionController
{
    @Autowired
    CollectionService collectionService;
    
    @RequestMapping(value="/getServiceName",method=RequestMethod.GET)
    public String getServiceName() throws ServiceUnavailableException
    {
        return collectionService.getServiceName();
    }
    
    @RequestMapping(value="/isServiceReady",method=RequestMethod.GET)
    public boolean isServiceReady() throws ServiceUnavailableException
    {
        return collectionService.isServiceReady();
    }
    
    @RequestMapping(value="/getServiceDependence",method=RequestMethod.GET)
    public String getServiceDependence() throws ServiceUnavailableException
    {
        return collectionService.getServiceDependence();
    }
    
    @RequestMapping(value="/startManageService",method=RequestMethod.POST)
    public void startManageService()
    {
        collectionService.startManageService();
    }
    
    @RequestMapping(value="/stopManageService",method=RequestMethod.POST)
    public void stopManageService()
    {
        collectionService.stopManageService();
    }
    
    @ResponseStatus(value=HttpStatus.NOT_FOUND, reason="webService is stop")
    @ExceptionHandler(ServiceUnavailableException.class)
    public void serviceUnavailableExceptionHandler() 
    {
        
    }
}
