import React from 'react';
import { Home, Building, PhoneCall, Users, Activity } from 'lucide-react';
import { ScraperStats } from '../types';

interface MetricCardsProps {
  leadsCount: number;
  agenciesCount: number;
  voiceCallsCount: number;
  tenantContactsCount: number;
  scraperStats: ScraperStats | null;
}

export const MetricCards: React.FC<MetricCardsProps> = ({
  leadsCount,
  agenciesCount,
  voiceCallsCount,
  tenantContactsCount,
  scraperStats,
}) => {
  const cards = [
    {
      id: 'metric-leads',
      label: 'Recent Leads',
      value: leadsCount,
      change: '+12 new today',
      icon: Home,
      color: 'text-blue-400',
      bg: 'bg-blue-500/10 border-blue-500/20',
    },
    {
      id: 'metric-agencies',
      label: 'Agencies',
      value: agenciesCount,
      change: 'Active brokers',
      icon: Building,
      color: 'text-purple-400',
      bg: 'bg-purple-500/10 border-purple-500/20',
    },
    {
      id: 'metric-calls',
      label: 'Voice Calls',
      value: voiceCallsCount,
      change: 'Outbound AI qualification',
      icon: PhoneCall,
      color: 'text-emerald-400',
      bg: 'bg-emerald-500/10 border-emerald-500/20',
    },
    {
      id: 'metric-tenants',
      label: 'Tenant Contacts',
      value: tenantContactsCount,
      change: 'Verified BG numbers',
      icon: Users,
      color: 'text-amber-400',
      bg: 'bg-amber-500/10 border-amber-500/20',
    },
    {
      id: 'metric-worker',
      label: 'Worker Status',
      value: scraperStats?.worker_status === 'running' ? 'Active' : 'Ready',
      change: `Last: ${scraperStats?.last_status ?? 'ok'} (${scraperStats?.last_total_scraped ?? 0} total)`,
      icon: Activity,
      color: scraperStats?.worker_status === 'running' ? 'text-amber-400' : 'text-cyan-400',
      bg: 'bg-cyan-500/10 border-cyan-500/20',
    },
  ];

  return (
    <div className="grid grid-cols-2 lg:grid-cols-5 gap-3.5 sm:gap-4 mb-6">
      {cards.map((card) => {
        const Icon = card.icon;
        return (
          <div
            key={card.id}
            id={card.id}
            className="p-4 rounded-xl bg-slate-800/60 border border-slate-700/60 shadow-sm flex flex-col justify-between"
          >
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-slate-400">{card.label}</span>
              <div className={`p-1.5 rounded-lg border ${card.bg}`}>
                <Icon className={`w-4 h-4 ${card.color}`} />
              </div>
            </div>
            <div>
              <div className="text-2xl font-bold text-white tracking-tight">{card.value}</div>
              <p className="text-[11px] text-slate-400 mt-0.5">{card.change}</p>
            </div>
          </div>
        );
      })}
    </div>
  );
};
