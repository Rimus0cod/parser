import React from 'react';
import { Activity, RefreshCw, CheckCircle2, AlertTriangle, Globe, Database, Play } from 'lucide-react';
import { ScraperStats } from '../types';

interface ScraperTabProps {
  stats: ScraperStats | null;
  onTriggerScrape: () => void;
  isScraping: boolean;
}

export const ScraperTab: React.FC<ScraperTabProps> = ({ stats, onTriggerScrape, isScraping }) => {
  const sites = [
    { name: 'imoti.bg', region: 'Bulgaria', active: true, desc: 'Primary Bulgarian residential rental portal' },
    { name: 'alo.bg', region: 'Bulgaria', active: true, desc: 'Classifieds & direct landlord listings' },
    { name: 'dom.ria.com', region: 'Ukraine', active: true, desc: 'Verified realtor listings & market data' },
    { name: 'olx.ua', region: 'Ukraine', active: true, desc: 'Direct owner ads with contact extraction' },
    { name: 'lun.ua', region: 'Ukraine', active: true, desc: 'New building developments & residential complexes' },
  ];

  return (
    <div className="space-y-6">
      {/* Worker State Overview */}
      <div className="bg-slate-800/60 border border-slate-700 rounded-xl p-5 shadow-sm">
        <div className="flex flex-wrap items-center justify-between gap-4 pb-4 border-b border-slate-700">
          <div>
            <div className="flex items-center space-x-2">
              <Activity className="w-5 h-5 text-blue-400" />
              <h3 className="text-base font-bold text-white">Async Scraper Worker Runtime</h3>
            </div>
            <p className="text-xs text-slate-400 mt-1">
              Background job scheduler running httpx multi-worker concurrent requests
            </p>
          </div>

          <button
            onClick={onTriggerScrape}
            disabled={isScraping}
            className={`inline-flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-semibold shadow transition ${
              isScraping
                ? 'bg-slate-700 text-slate-400 cursor-not-allowed'
                : 'bg-blue-600 hover:bg-blue-500 text-white active:scale-95'
            }`}
          >
            <Play className={`w-3.5 h-3.5 ${isScraping ? 'animate-spin' : ''}`} />
            <span>{isScraping ? 'Worker Scraping All Portals...' : 'Execute Scraper Run Now'}</span>
          </button>
        </div>

        {/* Status Metrics */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-4">
          <div className="p-3 bg-slate-900/80 rounded-lg border border-slate-700">
            <span className="text-xs text-slate-400 block mb-1">Worker State</span>
            <span className="text-sm font-bold text-emerald-400 uppercase font-mono">
              {isScraping ? 'RUNNING' : stats?.worker_status || 'IDLE'}
            </span>
          </div>

          <div className="p-3 bg-slate-900/80 rounded-lg border border-slate-700">
            <span className="text-xs text-slate-400 block mb-1">Last Completed Run</span>
            <span className="text-sm font-semibold text-slate-200">
              {stats?.last_finished_at ? new Date(stats.last_finished_at).toLocaleTimeString() : 'Just now'}
            </span>
          </div>

          <div className="p-3 bg-slate-900/80 rounded-lg border border-slate-700">
            <span className="text-xs text-slate-400 block mb-1">Total Scraped</span>
            <span className="text-sm font-bold text-blue-400 font-mono">
              {stats?.last_total_scraped || 0} ads
            </span>
          </div>

          <div className="p-3 bg-slate-900/80 rounded-lg border border-slate-700">
            <span className="text-xs text-slate-400 block mb-1">MySQL Written</span>
            <span className="text-sm font-bold text-purple-400 font-mono">
              +{stats?.last_written || 0} rows
            </span>
          </div>
        </div>
      </div>

      {/* Supported Portals */}
      <div className="bg-slate-800/50 border border-slate-700/60 rounded-xl overflow-hidden shadow-sm">
        <div className="p-3.5 border-b border-slate-700/60 flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <Globe className="w-4 h-4 text-cyan-400" />
            <h4 className="text-xs font-bold text-white uppercase tracking-wider">
              Configured Real Estate Portals (SCRAPER_SITES)
            </h4>
          </div>
          <span className="text-xs text-slate-400 font-mono">5 Active Sites</span>
        </div>

        <div className="divide-y divide-slate-700/50">
          {sites.map((site) => (
            <div key={site.name} className="p-3.5 flex items-center justify-between hover:bg-slate-700/20 transition">
              <div className="flex items-center space-x-3">
                <div className="w-2.5 h-2.5 rounded-full bg-emerald-400 shadow-sm shadow-emerald-500/50" />
                <div>
                  <div className="flex items-center space-x-2">
                    <span className="text-sm font-semibold text-white">{site.name}</span>
                    <span className="text-[10px] px-2 py-0.5 rounded bg-slate-900 text-slate-400 border border-slate-700">
                      {site.region}
                    </span>
                  </div>
                  <p className="text-xs text-slate-400 mt-0.5">{site.desc}</p>
                </div>
              </div>

              <div className="flex items-center space-x-3">
                <span className="text-xs text-emerald-400 font-medium">Ready</span>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Execution logs */}
      <div className="bg-slate-900 border border-slate-700/80 rounded-xl p-4 font-mono text-xs text-slate-300 space-y-1.5">
        <div className="text-slate-500 text-[11px] mb-2 border-b border-slate-800 pb-1">
          // Worker stdout & engine logs
        </div>
        <div>[2026-09-14 11:44:00] [INFO] MultiSiteScraper initialized with concurrency=5, timeout=15s</div>
        <div>[2026-09-14 11:44:01] [INFO] Loading agency phone blacklist from database (168 records loaded)</div>
        <div>[2026-09-14 11:44:02] [INFO] Worker polling Redis queue `scrape:worker_status`</div>
        {isScraping ? (
          <>
            <div className="text-amber-400 animate-pulse">
              [2026-09-14 11:45:00] [JOB] Manual scrape triggered via API /trigger-scrape...
            </div>
            <div className="text-cyan-400">[2026-09-14 11:45:01] [JOB] Fetching listings page 1 for imoti.bg, alo.bg...</div>
            <div className="text-blue-400">[2026-09-14 11:45:02] [JOB] Parsing HTML tags and regex phone matchers...</div>
          </>
        ) : (
          <div className="text-emerald-400">
            [2026-09-14 11:44:05] [STATUS] Worker idle, awaiting next cron interval or manual API trigger.
          </div>
        )}
      </div>
    </div>
  );
};
