package com.udacity.webcrawler.main;

import com.google.inject.Guice;
import com.udacity.webcrawler.WebCrawler;
import com.udacity.webcrawler.WebCrawlerModule;
import com.udacity.webcrawler.json.ConfigurationLoader;
import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.json.CrawlResultWriter;
import com.udacity.webcrawler.json.CrawlerConfiguration;
import com.udacity.webcrawler.profiler.Profiler;
import com.udacity.webcrawler.profiler.ProfilerModule;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

public final class WebCrawlerMain {

    private final CrawlerConfiguration config;

    private WebCrawlerMain(CrawlerConfiguration config) {
        this.config = Objects.requireNonNull(config);
    }

    @Inject
    private WebCrawler crawler;

    @Inject
    private Profiler profiler;

    private void run() throws Exception {
        Guice.createInjector(new WebCrawlerModule(config), new ProfilerModule()).injectMembers(this);

        CrawlResult result = crawler.crawl(config.getStartPages());
        CrawlResultWriter resultWriter = new CrawlResultWriter(result);
        // TODO: Write the crawl results to a JSON file (or System.out if the file name is empty)
        String resultPath = config.getResultPath();
        try (Writer outputWriter = resultPath.isEmpty() ?
                new OutputStreamWriter(System.out) :
                Files.newBufferedWriter(Paths.get(resultPath))) {
            resultWriter.write(outputWriter);
        } catch (IOException e) {
            System.err.println("Error writing crawl results: " + e.getMessage());
        }


        // TODO: Write the profile data to a text file (or System.out if the file name is empty)
        String profileOutputPath = config.getProfileOutputPath();
        try (Writer profileWriter = profileOutputPath.isEmpty() ?
                new OutputStreamWriter(System.out) :
                Files.newBufferedWriter(Paths.get(profileOutputPath))) {
            profiler.writeData(profileWriter);
        } catch (IOException e) {
            System.err.println("Error writing profile data: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: WebCrawlerMain [starting-url]");
            return;
        }

        CrawlerConfiguration config = new ConfigurationLoader(Path.of(args[0])).load();
        if (config == null) {
            System.err.println("Failed to load the configuration.");
            return;
        }
        new WebCrawlerMain(config).run();
    }
}
