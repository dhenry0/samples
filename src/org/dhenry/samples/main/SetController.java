package org.dhenry.samples.main;

import java.util.HashSet;
import java.util.Set;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class SetController {

    private static Set<String> set = new HashSet<>();

    @RequestMapping("/add", method=POST)
    public void add(@RequestParam(value="name") String name) {
      set.add(name);
    }
    
    @RequestMapping("/remove", method=POST)
    public void remove(@RequestParam(value="name") String name) {
      set.remove(name);
    }
    
    @RequestMapping("/size", method=GET)
    public SetSize size() {
      return new SetSize(set.size());
    }

    public static void main(String[] args) {
        SpringApplication.run(SetController.class, args);
    }
}