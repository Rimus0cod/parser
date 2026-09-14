import React from 'react';
import { Building2, RefreshCw, UserCheck, Server } from 'lucide-react';
import { HealthStatus } from '../types';

interface HeaderProps {
  health: HealthStatus | null;
  onTriggerScrape: () => void;
  isScraping: boolean;
}

export const Header: React.FC<HeaderProps> = ({ health, onTriggerScrape, isScraping }) => {
  return (
    <header className="border-b border-slate-800 bg-slate-900/80 backdrop-blur sticky top-0 z-30">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3.5 flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center space-x-3">
          <div className="w-10 h-10 rounded-lg bg-blue-600/20 border border-blue-500/30 flex items-center justify-center text-blue-400">
            <Building2 className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center space-x-2">
              <h1 className="text-lg font-bold text-white tracking-tight">Real Estate SaaS Core</h1>
              <span className="text-xs px-2 py-0.5 rounded-full bg-blue-900/60 text-blue-300 border border-blue-700/50 font-mono">
                v0.2.0
              </span>
            </div>
            <p className="text-xs text-slate-400">Production Real Estate Scraping & Voice Leads Platform</p>
          </div>
        </div>

        <div className="flex items-center space-x-3">
          {/* Status Indicator */}
          <div className="hidden sm:flex items-center space-x-2 text-xs px-3 py-1.5 rounded-md bg-slate-800/80 border border-slate-700">
            <Server className="w-3.5 h-3.5 text-emerald-400" />
            <span className="text-slate-400">API:</span>
            <span className="text-emerald-400 font-medium">{health ? 'Healthy' : 'Connecting...'}</span>
            <span className="text-slate-600">|</span>
            <span className="text-slate-400">Worker:</span>
            <span className={`font-medium ${isScraping ? 'text-amber-400' : 'text-emerald-400'}`}>
              {isScraping ? 'Scraping...' : 'Idle'}
            </span>
          </div>

          {/* Trigger Scrape Action */}
          <button
            id="header-trigger-scrape-btn"
            onClick={onTriggerScrape}
            disabled={isScraping}
            className={`inline-flex items-center space-x-2 px-3.5 py-1.5 rounded-md text-xs font-semibold shadow-sm transition ${
              isScraping
                ? 'bg-slate-700 text-slate-400 cursor-not-allowed'
                : 'bg-blue-600 hover:bg-blue-500 text-white active:scale-95'
            }`}
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isScraping ? 'animate-spin' : ''}`} />
            <span>{isScraping ? 'Scraping in progress...' : 'Trigger Scrape'}</span>
          </button>

          {/* User badge */}
          <div className="flex items-center space-x-2 text-xs px-3 py-1.5 rounded-md bg-slate-800 border border-slate-700/70 text-slate-300">
            <UserCheck className="w-3.5 h-3.5 text-blue-400" />
            <span className="font-medium">Pavlo (admin)</span>
          </div>
        </div>
      </div>
    </header>
  );
};
