import React, { useState, useEffect } from 'react';
import { Home, Building, PhoneCall, Users, Cpu } from 'lucide-react';
import { Header } from './components/Header';
import { MetricCards } from './components/MetricCards';
import { LeadsTab } from './components/LeadsTab';
import { AgenciesTab } from './components/AgenciesTab';
import { VoiceCallsTab } from './components/VoiceCallsTab';
import { TenantContactsTab } from './components/TenantContactsTab';
import { ScraperTab } from './components/ScraperTab';
import { Lead, LeadStatus, Agency, VoiceCall, TenantContact, ScraperStats, HealthStatus } from './types';

export function App() {
  const [activeTab, setActiveTab] = useState<'leads' | 'agencies' | 'voice' | 'tenants' | 'scraper'>('leads');
  const [leads, setLeads] = useState<Lead[]>([]);
  const [agencies, setAgencies] = useState<Agency[]>([]);
  const [voiceCalls, setVoiceCalls] = useState<VoiceCall[]>([]);
  const [tenantContacts, setTenantContacts] = useState<TenantContact[]>([]);
  const [scraperStats, setScraperStats] = useState<ScraperStats | null>(null);
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [isScraping, setIsScraping] = useState(false);
  const [isCalling, setIsCalling] = useState(false);

  // Fetch live data from backend API
  const refreshData = async () => {
    try {
      const [leadsRes, agenciesRes, callsRes, tenantsRes, statsRes, healthRes] = await Promise.allSettled([
        fetch('/api/leads').then((r) => (r.ok ? r.json() : Promise.reject())),
        fetch('/api/agencies').then((r) => (r.ok ? r.json() : Promise.reject())),
        fetch('/api/voice/calls').then((r) => (r.ok ? r.json() : Promise.reject())),
        fetch('/api/tenant-contacts').then((r) => (r.ok ? r.json() : Promise.reject())),
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
      // Individual requests are handled by Promise.allSettled; keep last known live data.
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
      const response = await fetch('/api/trigger-scrape', { method: 'POST' });
      if (!response.ok) throw new Error(`Failed to trigger scrape: ${response.status}`);
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
      if (!res.ok) {
        await refreshData();
        throw new Error(`Failed to start voice call: ${res.status}`);
      }
      const newCall = await res.json();
      setVoiceCalls((prev) => [newCall, ...prev]);
      setTimeout(async () => {
        await refreshData();
      }, 4500);
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
      const response = await fetch(`/api/leads/${encodeURIComponent(adId)}/status`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status }),
      });
      if (!response.ok) throw new Error(`Failed to update lead status: ${response.status}`);
    } catch {
      await refreshData();
    }
  };

  const handleImportTenants = async (rows: Array<{ name: string; phone: string; notes: string }>) => {
    const escapeCsv = (value: string) => `"${value.replace(/"/g, '""')}"`;
    const csv = [
      'name,phone,notes',
      ...rows.map((row) => [row.name, row.phone, row.notes].map(escapeCsv).join(',')),
    ].join('\n');

    const res = await fetch('/api/tenant-contacts/import?filename=dashboard.csv', {
      method: 'POST',
      headers: {
        'Content-Type': 'text/csv; charset=utf-8',
        'X-Filename': 'dashboard.csv',
      },
      body: csv,
    });
    if (!res.ok) {
      throw new Error(`Failed to import tenant contacts: ${res.status}`);
    }
    await refreshData();
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
          <span className="font-mono text-slate-400">React + Vite &bull; FastAPI backend</span>
        </div>
      </footer>
    </div>
  );
}

export default App;
