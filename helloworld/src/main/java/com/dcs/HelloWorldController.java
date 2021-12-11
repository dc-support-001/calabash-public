package com.dcs;

import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
public class HelloWorldController {
  @GetMapping()
  public Mono<String> sayHello() {
    return Mono.just("Hello world!");
  }
}
