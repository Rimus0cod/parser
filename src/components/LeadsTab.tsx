import React, { useState } from 'react';
import { Search, ExternalLink, PhoneCall, Filter, CheckCircle, Clock, Sparkles, CheckCircle2, ChevronDown } from 'lucide-react';
import { Lead, LeadStatus } from '../types';

interface LeadsTabProps {
  leads: Lead[];
  onStartVoiceCall: (adId: string) => Promise<void>;
  isCalling: boolean;
  onUpdateLeadStatus?: (adId: string, status: LeadStatus) => Promise<void>;
}

export const LeadsTab: React.FC<LeadsTabProps> = ({
  leads,
  onStartVoiceCall,
  isCalling,
  onUpdateLeadStatus,
}) => {
  const [search, setSearch] = useState('');
  const [selectedSource, setSelectedSource] = useState('all');
  const [selectedStatus, setSelectedStatus] = useState<string>('all');
  const [selectedLeadId, setSelectedLeadId] = useState<string | null>(leads[0]?.ad_id ?? null);
  const [callNotice, setCallNotice] = useState<string | null>(null);

  const sources = ['all', 'imoti.bg', 'alo.bg', 'dom.ria.com', 'olx.ua', 'lun.ua'];
  const statusOptions: LeadStatus[] = ['New', 'Contacted', 'Qualified', 'Pending'];

  // Status badge style resolver
  const getStatusBadgeConfig = (status: LeadStatus = 'New') => {
    switch (status) {
      case 'Qualified':
        return {
          bg: 'bg-emerald-950/80 text-emerald-300 border-emerald-700/80 hover:bg-emerald-900/80',
          dot: 'bg-emerald-400',
          icon: CheckCircle2,
          label: 'Qualified',
        };
      case 'Contacted':
        return {
          bg: 'bg-blue-950/80 text-blue-300 border-blue-700/80 hover:bg-blue-900/80',
          dot: 'bg-blue-400',
          icon: PhoneCall,
          label: 'Contacted',
        };
      case 'Pending':
        return {
          bg: 'bg-purple-950/80 text-purple-300 border-purple-700/80 hover:bg-purple-900/80',
          dot: 'bg-purple-400',
          icon: Clock,
          label: 'Pending',
        };
      case 'New':
      default:
        return {
          bg: 'bg-amber-950/80 text-amber-300 border-amber-700/80 hover:bg-amber-900/80',
          dot: 'bg-amber-400',
          icon: Sparkles,
          label: 'New',
        };
    }
  };

  const filteredLeads = leads.filter((lead) => {
    const matchesSearch =
      lead.title.toLowerCase().includes(search.toLowerCase()) ||
      lead.location.toLowerCase().includes(search.toLowerCase()) ||
      lead.seller_name.toLowerCase().includes(search.toLowerCase()) ||
      lead.phone.includes(search);
    const matchesSource = selectedSource === 'all' || lead.source_site === selectedSource;
    const currentStatus = lead.status || 'New';
    const matchesStatus = selectedStatus === 'all' || currentStatus === selectedStatus;
    return matchesSearch && matchesSource && matchesStatus;
  });

  const selectedLead = leads.find((l) => l.ad_id === selectedLeadId);

  const handleLaunchCall = async () => {
    if (!selectedLeadId) return;
    setCallNotice(`Initiating outbound voice qualification call for listing #${selectedLeadId}...`);
    try {
      await onStartVoiceCall(selectedLeadId);
      setCallNotice(`Voice call successfully queued and dialing for ${selectedLead?.seller_name || 'seller'}!`);
      setTimeout(() => setCallNotice(null), 5000);
    } catch {
      setCallNotice('Failed to start voice call. Please verify Twilio credentials or check service status.');
    }
  };

  const handleStatusChange = async (adId: string, newStatus: LeadStatus) => {
    if (onUpdateLeadStatus) {
      await onUpdateLeadStatus(adId, newStatus);
    }
  };

  return (
    <div className="space-y-4">
      {/* Controls Bar */}
      <div className="flex flex-col lg:flex-row items-stretch lg:items-center justify-between gap-3 bg-slate-800/40 p-3 rounded-xl border border-slate-700/60">
        <div className="flex items-center space-x-3 flex-1 max-w-md">
          <div className="relative w-full">
            <Search className="absolute left-3 top-2.5 w-4 h-4 text-slate-400" />
            <input
              id="leads-search-input"
              type="text"
              placeholder="Search title, location, phone, seller..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full pl-9 pr-3 py-2 bg-slate-900/90 border border-slate-700 rounded-lg text-sm text-slate-100 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          {/* Status Filter */}
          <div className="flex items-center space-x-2 text-xs text-slate-400">
            <Filter className="w-3.5 h-3.5 text-blue-400" />
            <span>Status:</span>
            <select
              id="leads-status-filter"
              value={selectedStatus}
              onChange={(e) => setSelectedStatus(e.target.value)}
              className="bg-slate-900 border border-slate-700 rounded-md px-2 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-blue-500"
            >
              <option value="all">All Statuses ({leads.length})</option>
              {statusOptions.map((st) => {
                const count = leads.filter((l) => (l.status || 'New') === st).length;
                return (
                  <option key={st} value={st}>
                    {st} ({count})
                  </option>
                );
              })}
            </select>
          </div>

          {/* Source Filter */}
          <div className="flex items-center space-x-2 text-xs text-slate-400">
            <span>Source:</span>
            <select
              id="leads-source-select"
              value={selectedSource}
              onChange={(e) => setSelectedSource(e.target.value)}
              className="bg-slate-900 border border-slate-700 rounded-md px-2 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-blue-500"
            >
              {sources.map((s) => (
                <option key={s} value={s}>
                  {s === 'all' ? 'All Sources' : s}
                </option>
              ))}
            </select>
          </div>

          <button
            id="start-voice-call-selected-btn"
            onClick={handleLaunchCall}
            disabled={!selectedLeadId || isCalling}
            className={`inline-flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-semibold shadow-sm transition ${
              !selectedLeadId || isCalling
                ? 'bg-slate-700 text-slate-400 cursor-not-allowed'
                : 'bg-emerald-600 hover:bg-emerald-500 text-white active:scale-95'
            }`}
          >
            <PhoneCall className={`w-3.5 h-3.5 ${isCalling ? 'animate-bounce' : ''}`} />
            <span>{isCalling ? 'Dialing Lead...' : 'Start Voice Call For Selected Lead'}</span>
          </button>
        </div>
      </div>

      {callNotice && (
        <div className="p-3 bg-emerald-950/60 border border-emerald-800/80 rounded-lg text-emerald-200 text-xs flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <CheckCircle className="w-4 h-4 text-emerald-400 shrink-0" />
            <span>{callNotice}</span>
          </div>
          <span className="text-[10px] text-emerald-400/80">Check the Voice Calls tab for live transcript</span>
        </div>
      )}

      {/* Selected Lead Quick Bar */}
      {selectedLead && (
        <div className="bg-slate-900/80 border border-slate-700/80 rounded-xl p-3 px-4 flex flex-wrap items-center justify-between gap-3 text-xs">
          <div className="flex items-center space-x-3">
            <span className="text-slate-400 font-medium">Selected:</span>
            <span className="text-white font-semibold">{selectedLead.title}</span>
            <span className="text-slate-400 font-mono">({selectedLead.ad_id})</span>
          </div>

          <div className="flex items-center space-x-2">
            <span className="text-slate-400 text-[11px]">Quick Status:</span>
            {statusOptions.map((st) => {
              const current = (selectedLead.status || 'New') === st;
              const cfg = getStatusBadgeConfig(st);
              return (
                <button
                  key={st}
                  onClick={() => handleStatusChange(selectedLead.ad_id, st)}
                  className={`px-2.5 py-1 rounded text-[11px] font-medium border transition ${
                    current
                      ? `${cfg.bg} ring-1 ring-white/20 font-semibold shadow-sm`
                      : 'bg-slate-800/80 text-slate-400 border-slate-700 hover:text-slate-200 hover:bg-slate-700/50'
                  }`}
                >
                  {st}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Leads Table */}
      <div className="bg-slate-800/50 border border-slate-700/60 rounded-xl overflow-hidden shadow-sm">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-900/80 text-slate-400 border-b border-slate-700/80 uppercase font-semibold">
              <tr>
                <th className="py-3 px-3 w-10 text-center">Select</th>
                <th className="py-3 px-3 w-28">Status</th>
                <th className="py-3 px-3">Ad ID</th>
                <th className="py-3 px-3">Title & Location</th>
                <th className="py-3 px-3">Price</th>
                <th className="py-3 px-3">Size</th>
                <th className="py-3 px-3">Source</th>
                <th className="py-3 px-3">Contact</th>
                <th className="py-3 px-3">Phone</th>
                <th className="py-3 px-3 text-right">Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {filteredLeads.length === 0 ? (
                <tr>
                  <td colSpan={10} className="py-8 text-center text-slate-400">
                    No leads found matching your search.
                  </td>
                </tr>
              ) : (
                filteredLeads.map((lead) => {
                  const isSelected = selectedLeadId === lead.ad_id;
                  const currentStatus = lead.status || 'New';
                  const badgeCfg = getStatusBadgeConfig(currentStatus);
                  const Icon = badgeCfg.icon;

                  return (
                    <tr
                      key={lead.ad_id}
                      onClick={() => setSelectedLeadId(lead.ad_id)}
                      className={`cursor-pointer transition ${
                        isSelected ? 'bg-blue-900/30 text-white' : 'hover:bg-slate-700/30 text-slate-300'
                      }`}
                    >
                      <td className="py-3 px-3 text-center">
                        <input
                          type="radio"
                          name="selectedLead"
                          checked={isSelected}
                          onChange={() => setSelectedLeadId(lead.ad_id)}
                          className="h-3.5 w-3.5 text-blue-600 focus:ring-blue-500 border-slate-600 bg-slate-800"
                        />
                      </td>

                      {/* Dynamic Status Badge */}
                      <td className="py-3 px-3" onClick={(e) => e.stopPropagation()}>
                        <div className="relative group inline-block" title="Click to change lead status">
                          <span
                            className={`inline-flex items-center space-x-1.5 px-2.5 py-1 rounded-full text-[11px] font-semibold border transition shadow-sm ${badgeCfg.bg}`}
                          >
                            <span className={`w-1.5 h-1.5 rounded-full ${badgeCfg.dot} animate-pulse`} />
                            <Icon className="w-3 h-3 shrink-0" />
                            <span>{badgeCfg.label}</span>
                            <ChevronDown className="w-2.5 h-2.5 opacity-60 ml-0.5" />
                          </span>

                          <select
                            value={currentStatus}
                            onChange={(e) => handleStatusChange(lead.ad_id, e.target.value as LeadStatus)}
                            className="absolute inset-0 w-full h-full opacity-0 cursor-pointer text-xs"
                            title="Select status"
                          >
                            <option value="New" className="bg-slate-900 text-amber-300">New</option>
                            <option value="Contacted" className="bg-slate-900 text-blue-300">Contacted</option>
                            <option value="Qualified" className="bg-slate-900 text-emerald-300">Qualified</option>
                            <option value="Pending" className="bg-slate-900 text-purple-300">Pending</option>
                          </select>
                        </div>
                      </td>

                      <td className="py-3 px-3 font-mono text-[11px] text-slate-400 font-medium">
                        {lead.ad_id}
                      </td>
                      <td className="py-3 px-3 max-w-xs">
                        <div className="font-medium text-slate-100 truncate">{lead.title}</div>
                        <div className="text-[11px] text-slate-400">{lead.location}</div>
                      </td>
                      <td className="py-3 px-3 font-semibold text-emerald-400">{lead.price}</td>
                      <td className="py-3 px-3 text-slate-400">{lead.size}</td>
                      <td className="py-3 px-3">
                        <span className="px-2 py-0.5 rounded bg-slate-900 text-slate-300 border border-slate-700 text-[10px]">
                          {lead.source_site}
                        </span>
                      </td>
                      <td className="py-3 px-3">
                        <div className="text-slate-200">{lead.contact_name || lead.seller_name}</div>
                        <div className="text-[10px] text-slate-400">{lead.ad_type}</div>
                      </td>
                      <td className="py-3 px-3 font-mono text-slate-200">{lead.phone}</td>
                      <td className="py-3 px-3 text-right">
                        <a
                          href={lead.link}
                          target="_blank"
                          rel="noreferrer"
                          onClick={(e) => e.stopPropagation()}
                          className="inline-flex items-center space-x-1 text-blue-400 hover:text-blue-300 p-1 rounded hover:bg-slate-700/50"
                          title="Open Original Listing"
                        >
                          <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
