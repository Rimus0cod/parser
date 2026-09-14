import React, { useState } from 'react';
import { PhoneCall, Play, FileText, CheckCircle2, AlertCircle, Clock, Volume2, PhoneIncoming } from 'lucide-react';
import { VoiceCall } from '../types';

interface VoiceCallsTabProps {
  calls: VoiceCall[];
}

export const VoiceCallsTab: React.FC<VoiceCallsTabProps> = ({ calls }) => {
  const [selectedCallId, setSelectedCallId] = useState<number>(calls[0]?.id ?? 1);

  const selectedCall = calls.find((c) => c.id === selectedCallId) || calls[0];

  const getStatusBadge = (status: VoiceCall['status']) => {
    switch (status) {
      case 'completed':
        return (
          <span className="inline-flex items-center space-x-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-emerald-950 text-emerald-300 border border-emerald-800">
            <CheckCircle2 className="w-3 h-3" />
            <span>Completed</span>
          </span>
        );
      case 'in-progress':
        return (
          <span className="inline-flex items-center space-x-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-blue-950 text-blue-300 border border-blue-800 animate-pulse">
            <Clock className="w-3 h-3 animate-spin" />
            <span>In-Progress</span>
          </span>
        );
      case 'no-answer':
      case 'failed':
        return (
          <span className="inline-flex items-center space-x-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-rose-950 text-rose-300 border border-rose-800">
            <AlertCircle className="w-3 h-3" />
            <span>{status}</span>
          </span>
        );
      default:
        return (
          <span className="inline-flex items-center space-x-1 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-slate-800 text-slate-300 border border-slate-700">
            <span>{status}</span>
          </span>
        );
    }
  };

  return (
    <div className="space-y-6">
      {/* Top list */}
      <div className="bg-slate-800/50 border border-slate-700/60 rounded-xl overflow-hidden shadow-sm">
        <div className="p-3.5 border-b border-slate-700/60 flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <PhoneCall className="w-4 h-4 text-emerald-400" />
            <h3 className="text-sm font-semibold text-white">Outbound AI Voice Call Sessions</h3>
          </div>
          <span className="text-xs text-slate-400 font-mono">{calls.length} Total Calls</span>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-900/80 text-slate-400 border-b border-slate-700/80 uppercase font-semibold">
              <tr>
                <th className="py-2.5 px-3 w-12">Call ID</th>
                <th className="py-2.5 px-3">Status</th>
                <th className="py-2.5 px-3">Listing Title</th>
                <th className="py-2.5 px-3">Broker Contact</th>
                <th className="py-2.5 px-3">Phone (E.164)</th>
                <th className="py-2.5 px-3">Answers Extracted</th>
                <th className="py-2.5 px-3">Date</th>
                <th className="py-2.5 px-3 text-right">Inspect</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {calls.map((call) => {
                const isSelected = selectedCall?.id === call.id;
                const answerSummary = Object.entries(call.answers_json || {})
                  .map(([k, v]) => `${k}: ${v}`)
                  .join(', ') || '-';

                return (
                  <tr
                    key={call.id}
                    onClick={() => setSelectedCallId(call.id)}
                    className={`cursor-pointer transition ${
                      isSelected ? 'bg-emerald-950/30 text-white' : 'hover:bg-slate-700/30 text-slate-300'
                    }`}
                  >
                    <td className="py-2.5 px-3 font-mono font-bold text-slate-400">#{call.id}</td>
                    <td className="py-2.5 px-3">{getStatusBadge(call.status)}</td>
                    <td className="py-2.5 px-3 font-medium max-w-xs truncate text-slate-200">
                      {call.listing_title || `Listing #${call.listing_ad_id}`}
                    </td>
                    <td className="py-2.5 px-3 text-slate-300">{call.contact_name}</td>
                    <td className="py-2.5 px-3 font-mono text-slate-300">{call.phone_e164}</td>
                    <td className="py-2.5 px-3 max-w-xs truncate text-slate-400" title={answerSummary}>
                      {answerSummary}
                    </td>
                    <td className="py-2.5 px-3 text-slate-400">
                      {call.created_at ? new Date(call.created_at).toLocaleTimeString() : '-'}
                    </td>
                    <td className="py-2.5 px-3 text-right">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedCallId(call.id);
                        }}
                        className={`px-2.5 py-1 rounded text-[11px] font-medium transition ${
                          isSelected
                            ? 'bg-emerald-600 text-white'
                            : 'bg-slate-700 hover:bg-slate-600 text-slate-200'
                        }`}
                      >
                        Inspect
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Inspector Panel for Selected Call */}
      {selectedCall && (
        <div className="bg-slate-800/60 border border-slate-700 rounded-xl p-5 shadow-sm space-y-5">
          <div className="flex flex-wrap items-center justify-between gap-3 pb-3 border-b border-slate-700">
            <div>
              <div className="flex items-center space-x-3">
                <h4 className="text-base font-bold text-white">
                  Voice Call Inspection #{selectedCall.id}
                </h4>
                {getStatusBadge(selectedCall.status)}
              </div>
              <p className="text-xs text-slate-400 mt-0.5">
                Target: {selectedCall.listing_title} ({selectedCall.listing_ad_id})
              </p>
            </div>

            <div className="flex items-center space-x-2">
              <span className="text-xs text-slate-400 font-mono">
                Initiated by: {selectedCall.initiated_by}
              </span>
            </div>
          </div>

          {/* Quick Metrics Grid */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
            <div className="p-3 bg-slate-900/80 rounded-lg border border-slate-700/60">
              <span className="text-slate-400 block mb-1">Phone Number</span>
              <span className="font-mono text-slate-200 font-medium">{selectedCall.phone_e164}</span>
            </div>
            <div className="p-3 bg-slate-900/80 rounded-lg border border-slate-700/60">
              <span className="text-slate-400 block mb-1">Script Model</span>
              <span className="text-slate-200 font-medium">{selectedCall.script_name}</span>
            </div>
            <div className="p-3 bg-slate-900/80 rounded-lg border border-slate-700/60">
              <span className="text-slate-400 block mb-1">Started At</span>
              <span className="text-slate-200">{selectedCall.started_at ? new Date(selectedCall.started_at).toLocaleTimeString() : '-'}</span>
            </div>
            <div className="p-3 bg-slate-900/80 rounded-lg border border-slate-700/60">
              <span className="text-slate-400 block mb-1">Duration</span>
              <span className="text-slate-200">
                {selectedCall.completed_at && selectedCall.started_at
                  ? `${Math.round((new Date(selectedCall.completed_at).getTime() - new Date(selectedCall.started_at).getTime()) / 1000)}s`
                  : 'N/A'}
              </span>
            </div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* Structured Answers */}
            <div className="space-y-2">
              <div className="flex items-center space-x-2 text-xs font-semibold text-slate-300">
                <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                <span>Extracted Listing Answers (Structured JSON)</span>
              </div>
              <div className="p-3.5 bg-slate-900 rounded-lg border border-slate-700 font-mono text-xs overflow-x-auto">
                <pre className="text-emerald-400">
                  {JSON.stringify(selectedCall.answers_json, null, 2)}
                </pre>
              </div>
            </div>

            {/* Transcript */}
            <div className="space-y-2">
              <div className="flex items-center space-x-2 text-xs font-semibold text-slate-300">
                <FileText className="w-3.5 h-3.5 text-blue-400" />
                <span>Call Dialogue & Audio Transcript</span>
              </div>
              <div className="p-3.5 bg-slate-900 rounded-lg border border-slate-700 text-xs text-slate-300 whitespace-pre-wrap leading-relaxed h-[180px] overflow-y-auto">
                {selectedCall.transcript || (
                  <span className="text-slate-500 italic">No transcript recorded for this session.</span>
                )}
              </div>
            </div>
          </div>

          {/* Audio recording player simulation */}
          {selectedCall.recording_url && (
            <div className="p-3 bg-slate-900/70 border border-slate-700/80 rounded-lg flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <div className="p-2 rounded-md bg-blue-600/20 text-blue-400">
                  <Volume2 className="w-4 h-4" />
                </div>
                <div>
                  <span className="text-xs font-semibold text-white block">Twilio Audio Recording Archive</span>
                  <span className="text-[11px] text-slate-400 font-mono">{selectedCall.recording_url}</span>
                </div>
              </div>
              <button
                onClick={() => alert(`Simulating playback for recording: ${selectedCall.recording_url}`)}
                className="inline-flex items-center space-x-1.5 px-3 py-1.5 bg-slate-800 hover:bg-slate-700 border border-slate-600 rounded text-xs font-medium text-slate-200 transition"
              >
                <Play className="w-3 h-3 text-emerald-400" />
                <span>Play Audio</span>
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
};
