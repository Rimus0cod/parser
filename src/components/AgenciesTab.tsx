import React, { useState } from 'react';
import { Search, Building, Phone, Mail, ExternalLink, MapPin } from 'lucide-react';
import { Agency } from '../types';

interface AgenciesTabProps {
  agencies: Agency[];
}

export const AgenciesTab: React.FC<AgenciesTabProps> = ({ agencies }) => {
  const [search, setSearch] = useState('');
  const [selectedCity, setSelectedCity] = useState('all');

  const cities = ['all', ...Array.from(new Set(agencies.map((a) => a.city).filter(Boolean)))];

  const filteredAgencies = agencies.filter((agency) => {
    const matchesSearch =
      agency.agency_name.toLowerCase().includes(search.toLowerCase()) ||
      agency.phones.includes(search) ||
      agency.contact_name.toLowerCase().includes(search.toLowerCase()) ||
      agency.email.toLowerCase().includes(search.toLowerCase());
    const matchesCity = selectedCity === 'all' || agency.city === selectedCity;
    return matchesSearch && matchesCity;
  });

  return (
    <div className="space-y-4">
      {/* Search & Filter */}
      <div className="flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3 bg-slate-800/40 p-3 rounded-xl border border-slate-700/60">
        <div className="flex items-center space-x-3 flex-1 max-w-md">
          <div className="relative w-full">
            <Search className="absolute left-3 top-2.5 w-4 h-4 text-slate-400" />
            <input
              id="agencies-search-input"
              type="text"
              placeholder="Search agency name, phone, contact..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full pl-9 pr-3 py-2 bg-slate-900/90 border border-slate-700 rounded-lg text-sm text-slate-100 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-purple-500"
            />
          </div>
        </div>

        <div className="flex items-center space-x-2 text-xs text-slate-400">
          <span>City:</span>
          <select
            id="agencies-city-select"
            value={selectedCity}
            onChange={(e) => setSelectedCity(e.target.value)}
            className="bg-slate-900 border border-slate-700 rounded-md px-2 py-1.5 text-xs text-slate-200 focus:outline-none"
          >
            {cities.map((city) => (
              <option key={city} value={city}>
                {city === 'all' ? 'All Cities' : city}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Agencies Table */}
      <div className="bg-slate-800/50 border border-slate-700/60 rounded-xl overflow-hidden shadow-sm">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-900/80 text-slate-400 border-b border-slate-700/80 uppercase font-semibold">
              <tr>
                <th className="py-3 px-3 w-12">#</th>
                <th className="py-3 px-3">Agency Name</th>
                <th className="py-3 px-3">City</th>
                <th className="py-3 px-3">Phone</th>
                <th className="py-3 px-3">Contact Person</th>
                <th className="py-3 px-3">Email</th>
                <th className="py-3 px-3 text-right">Profile</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {filteredAgencies.length === 0 ? (
                <tr>
                  <td colSpan={7} className="py-8 text-center text-slate-400">
                    No agencies found.
                  </td>
                </tr>
              ) : (
                filteredAgencies.map((agency, idx) => (
                  <tr key={agency.id || idx} className="hover:bg-slate-700/30 text-slate-300 transition">
                    <td className="py-3 px-3 text-slate-500 font-mono">{idx + 1}</td>
                    <td className="py-3 px-3">
                      <div className="font-semibold text-slate-100 flex items-center space-x-2">
                        <Building className="w-3.5 h-3.5 text-purple-400 shrink-0" />
                        <span>{agency.agency_name}</span>
                      </div>
                    </td>
                    <td className="py-3 px-3">
                      <span className="inline-flex items-center space-x-1 text-slate-300">
                        <MapPin className="w-3 h-3 text-slate-400" />
                        <span>{agency.city || 'София'}</span>
                      </span>
                    </td>
                    <td className="py-3 px-3 font-mono text-slate-200">
                      <div className="flex items-center space-x-1.5">
                        <Phone className="w-3 h-3 text-slate-400" />
                        <span>{agency.phones}</span>
                      </div>
                    </td>
                    <td className="py-3 px-3 text-slate-300">{agency.contact_name || '-'}</td>
                    <td className="py-3 px-3">
                      <div className="flex items-center space-x-1.5 text-slate-400">
                        <Mail className="w-3 h-3 text-slate-500" />
                        <span>{agency.email || 'office@imoti.bg'}</span>
                      </div>
                    </td>
                    <td className="py-3 px-3 text-right">
                      {agency.profile_url ? (
                        <a
                          href={agency.profile_url}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center space-x-1 text-purple-400 hover:text-purple-300 p-1 rounded hover:bg-slate-700/50"
                        >
                          <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                      ) : (
                        <span className="text-slate-600">-</span>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
