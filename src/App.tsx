import React, { useState, useEffect } from 'react';
import { Home, Building, PhoneCall, Users, Cpu, Layers } from 'lucide-react';
import { Header } from './components/Header';
import { MetricCards } from './components/MetricCards';
import { LeadsTab } from './components/LeadsTab';
import { AgenciesTab } from './components/AgenciesTab';
import { VoiceCallsTab } from './components/VoiceCallsTab';
import { TenantContactsTab } from './components/TenantContactsTab';
import { ScraperTab } from './components/ScraperTab';
import { Lead, LeadStatus, Agency, VoiceCall, TenantContact, ScraperStats, HealthStatus } from './types';
import {
  initialLeads,
  initialAgencies,
  initialVoiceCalls,
  initialTenantContacts,
  initialScraperStats,
} from './data/initialData';

export function App() {
  const [activeTab, setActiveTab] = useState<'leads' | 'agencies' | 'voice' | 'tenants' | 'scraper'>('leads');
  const [leads, setLeads] = useState<Lead[]>(initialLeads);
  const [agencies, setAgencies] = useState<Agency[]>(initialAgencies);
  const [voiceCalls, setVoiceCalls] = useState<VoiceCall[]>(initialVoiceCalls);
  const [tenantContacts, setTenantContacts] = useState<TenantContact[]>(initialTenantContacts);
  const [scraperStats, setScraperStats] = useState<ScraperStats | null>(initialScraperStats);
  const [health, setHealth] = useState<HealthStatus | null>({
    status: 'ok',
    app: 'Real Estate SaaS Core',
    redis: 'ok (in-memory)',
    database: 'ok (in-memory)',
    uptime: 120,
    timestamp: new Date().toISOString(),
  });
  const [isScraping, setIsScraping] = useState(false);
  const [isCalling, setIsCalling] = useState(false);

  // Fetch live data from backend API
  const refreshData = async () => {
    try {
      const [leadsRes, agenciesRes, callsRes, tenantsRes, statsRes, healthRes] = await Promise.allSettled([
        fetch('/api/leads').then((r) => (r.ok ? r.json() : Promise.reject())),
        fetch('/api/agencies').then((r) => (r.ok ? r.json() : Promise.reject())),
        fetch('/api/voice/calls').then((r) => (r.ok ? r.json() : Promise.reject())),
        fetch('/api/voice/tenants').then((r) => (r.ok ? r.json() : Promise.reject())),
        fetch('/api/scraper/stats').then((r) => (r.ok ? r.json() : Promise.reject())),
        fetch('/api/health').then((r) => (r.ok ? r.json() : Promise.reject())),
      ]);

      if (leadsRes.status === 'fulfilled' && leadsRes.value) setLeads(leadsRes.value);
      if (agenciesRes.status === 'fulfilled' && agenciesRes.value) setAgencies(agenciesRes.value);
      if (callsRes.status === 'fulfilled' && callsRes.value) setVoiceCalls(callsRes.value);
      if (tenantsRes.status === 'fulfilled' && tenantsRes.value) setTenantContacts(tenantsRes.value);
      if (statsRes.status === 'fulfilled' && statsRes.value) setScraperStats(statsRes.value);
      if (healthRes.status === 'fulfilled' && healthRes.value) setHealth(healthRes.value);
    } catch {
      // Fallback already preloaded
    }
  };

  useEffect(() => {
    refreshData();
    const interval = setInterval(refreshData, 10000);
    return () => clearInterval(interval);
  }, []);

  const handleTriggerScrape = async () => {
    setIsScraping(true);
    try {
      await fetch('/api/trigger-scrape', { method: 'POST' });
      setTimeout(async () => {
        await refreshData();
        setIsScraping(false);
      }, 3500);
    } catch {
      setIsScraping(false);
    }
  };

  const handleStartVoiceCall = async (listingAdId: string) => {
    setIsCalling(true);
    // Optimistically update lead status to Contacted
    setLeads((prev) =>
      prev.map((l) => (l.ad_id === listingAdId ? { ...l, status: 'Contacted' } : l))
    );
    try {
      const res = await fetch('/api/voice/calls', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ listing_ad_id: listingAdId, initiated_by: 'Pavlo (admin)' }),
      });
      if (res.ok) {
        const newCall = await res.json();
        setVoiceCalls((prev) => [newCall, ...prev]);
        setTimeout(async () => {
          await refreshData();
        }, 4500);
      }
    } finally {
      setIsCalling(false);
    }
  };

  const handleUpdateLeadStatus = async (adId: string, status: LeadStatus) => {
    // Optimistic UI update
    setLeads((prev) =>
      prev.map((l) => (l.ad_id === adId ? { ...l, status } : l))
    );
    try {
      await fetch(`/api/leads/${encodeURIComponent(adId)}/status`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status }),
      });
    } catch {
      await refreshData();
    }
  };

  const handleImportTenants = async (rows: Array<{ name: string; phone: string; notes: string }>) => {
    try {
      const res = await fetch('/api/voice/tenants/import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(rows),
      });
      if (res.ok) {
        await refreshData();
      }
    } catch {
      // fallback
    }
  };

  interface TabItem {
    id: 'leads' | 'agencies' | 'voice' | 'tenants' | 'scraper';
    label: string;
    icon: React.ComponentType<{ className?: string }>;
    count?: number;
  }

  const tabs: TabItem[] = [
    { id: 'leads', label: 'Recent Leads', icon: Home, count: leads.length },
    { id: 'agencies', label: 'Agencies', icon: Building, count: agencies.length },
    { id: 'voice', label: 'Voice Calls', icon: PhoneCall, count: voiceCalls.length },
    { id: 'tenants', label: 'Tenant Contacts', icon: Users, count: tenantContacts.length },
    { id: 'scraper', label: 'Scraper & Worker', icon: Cpu },
  ];

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans">
      <Header health={health} onTriggerScrape={handleTriggerScrape} isScraping={isScraping} />

      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {/* Metric Cards Overview */}
        <MetricCards
          leadsCount={leads.length}
          agenciesCount={agencies.length}
          voiceCallsCount={voiceCalls.length}
          tenantContactsCount={tenantContacts.length}
          scraperStats={scraperStats}
        />

        {/* Tab Navigation */}
        <div className="border-b border-slate-800 mb-6">
          <div className="flex space-x-1 sm:space-x-3 overflow-x-auto pb-px">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.id;
              return (
                <button
                  key={tab.id}
                  id={`tab-${tab.id}`}
                  onClick={() => setActiveTab(tab.id)}
                  className={`inline-flex items-center space-x-2 py-3 px-3 sm:px-4 text-xs sm:text-sm font-semibold border-b-2 whitespace-nowrap transition ${
                    isActive
                      ? 'border-blue-500 text-blue-400 bg-blue-500/10 rounded-t-lg'
                      : 'border-transparent text-slate-400 hover:text-slate-200 hover:border-slate-700'
                  }`}
                >
                  <Icon className="w-4 h-4" />
                  <span>{tab.label}</span>
                  {tab.count !== undefined && (
                    <span
                      className={`text-[10px] px-1.5 py-0.2 rounded-full font-mono ${
                        isActive ? 'bg-blue-600 text-white' : 'bg-slate-800 text-slate-400'
                      }`}
                    >
                      {tab.count}
                    </span>
                  )}
                </button>
              );
            })}
          </div>
        </div>

        {/* Tab Content */}
        {activeTab === 'leads' && (
          <LeadsTab
            leads={leads}
            onStartVoiceCall={handleStartVoiceCall}
            isCalling={isCalling}
            onUpdateLeadStatus={handleUpdateLeadStatus}
          />
        )}

        {activeTab === 'agencies' && <AgenciesTab agencies={agencies} />}

        {activeTab === 'voice' && <VoiceCallsTab calls={voiceCalls} />}

        {activeTab === 'tenants' && (
          <TenantContactsTab contacts={tenantContacts} onImportTenants={handleImportTenants} />
        )}

        {activeTab === 'scraper' && (
          <ScraperTab stats={scraperStats} onTriggerScrape={handleTriggerScrape} isScraping={isScraping} />
        )}
      </main>

      <footer className="border-t border-slate-800/80 py-4 text-center text-xs text-slate-500 bg-slate-900/60">
        <div className="max-w-7xl mx-auto px-4 flex flex-col sm:flex-row items-center justify-between gap-2">
          <span>Real Estate SaaS Core &copy; 2026. Production lead scanner and outbound voice qualification system.</span>
          <span className="font-mono text-slate-400">Node.js 22 Runtime &bull; Express + Vite SPA</span>
        </div>
      </footer>
    </div>
  );
}

export default App;
