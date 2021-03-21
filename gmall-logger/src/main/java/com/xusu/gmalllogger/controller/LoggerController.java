package com.xusu.gmalllogger.controller;


import com.oracle.tools.packager.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("test1")
    public String test1(){
        System.out.println("1111111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name){
        System.out.println(name);
        return "success";
    }

    @RequestMapping("applog")
    public String applog(@RequestParam("param") String logs){
        Log.info(logs);

    //    kafkaTemplate.send("ods_base_log",logs);
        return "success";
    }
}
