package org.tweet.sentiment.analyis.fetcher.controller;

import org.tweet.sentiment.analyis.fetcher.model.Term;
import org.tweet.sentiment.analyis.fetcher.service.RegistrationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
public class RegistrationController {

    private final RegistrationService registrationService;

    @Autowired
    public RegistrationController(RegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    @CrossOrigin
    @RequestMapping(value = "/terms", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void consumeTerm(@RequestBody Term term) {
        this.registrationService.consumeTweets(term.getIdentifier());
    }

    @CrossOrigin
    @RequestMapping(value = "/terms/stop", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void stopConsuming() {
        this.registrationService.stopConsuming();
    }
}
