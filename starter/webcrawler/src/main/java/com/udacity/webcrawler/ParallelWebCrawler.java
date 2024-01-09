package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
    private final Clock clock;
    private final Duration timeout;
    private final int popularWordCount;
    private final int maxDepth;
    private final Set<Pattern> ignoredUrls;
    private final ForkJoinPool pool;
    private final PageParserFactory parserFactory;

    @Inject
    ParallelWebCrawler(
            Clock clock,
            @Timeout Duration timeout,
            @PopularWordCount int popularWordCount,
            @MaxDepth int maxDepth,
            @IgnoredUrls Set<Pattern> ignoredUrls,
            @TargetParallelism int threadCount,
            PageParserFactory parserFactory) {
        this.clock = clock;
        this.timeout = timeout;
        this.popularWordCount = popularWordCount;
        this.maxDepth = maxDepth;
        this.ignoredUrls = ignoredUrls;
        this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
        this.parserFactory = parserFactory;
    }

    @Override
    public CrawlResult crawl(List<String> startingUrls) {
        Instant deadline = clock.instant().plus(timeout);

        ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();
        ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();

        startingUrls.forEach(url -> pool.invoke(new InternalCrawler(url, deadline, maxDepth, counts, visitedUrls, ignoredUrls)));

        Map<String, Integer> sortedCounts = counts.isEmpty() ? counts : WordCounts.sort(counts, popularWordCount);

        return new CrawlResult.Builder()
                .setWordCounts(sortedCounts)
                .setUrlsVisited(visitedUrls.size())
                .build();
    }

    private class InternalCrawler extends RecursiveTask<Boolean> {
        private final String url;
        private final Instant deadline;
        private final int maxDepth;
        private final ConcurrentMap<String, Integer> counts;
        private final ConcurrentSkipListSet<String> visitedUrls;
        private final Set<Pattern> ignoredUrls;

        InternalCrawler(String url, Instant deadline, int maxDepth, ConcurrentMap<String, Integer> counts, ConcurrentSkipListSet<String> visitedUrls, Set<Pattern> ignoredUrls) {
            this.url = url;
            this.deadline = deadline;
            this.maxDepth = maxDepth;
            this.counts = counts;
            this.visitedUrls = visitedUrls;
            this.ignoredUrls = ignoredUrls;
        }

        @Override
        protected Boolean compute() {
            if (shouldSkipUrl()) {
                return false;
            }
            processPage();
            return true;
        }

        private boolean shouldSkipUrl() {
            return maxDepth == 0
                    || clock.instant().isAfter(deadline)
                    || ignoredUrls.stream().anyMatch(pattern -> pattern.matcher(url).matches())
                    || !visitedUrls.add(url);
        }

        private void processPage() {
            PageParser.Result result = parserFactory.get(url).parse();
            result.getWordCounts().forEach((word, count) ->
                    counts.merge(word, count, Integer::sum));

            List<InternalCrawler> subtasks = result.getLinks().stream()
                    .map(link -> new InternalCrawler(link, deadline, maxDepth - 1, counts, visitedUrls, ignoredUrls))
                    .collect(Collectors.toList());
            invokeAll(subtasks);
        }
    }

    @Override
    public int getMaxParallelism() {
        return Runtime.getRuntime().availableProcessors();
    }
}
