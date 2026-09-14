import React, { useState } from 'react';
import { Users, Upload, Plus, CheckCircle, Search, Phone, FileText } from 'lucide-react';
import { TenantContact } from '../types';

interface TenantContactsTabProps {
  contacts: TenantContact[];
  onImportTenants: (rows: Array<{ name: string; phone: string; notes: string }>) => Promise<void>;
}

export const TenantContactsTab: React.FC<TenantContactsTabProps> = ({ contacts, onImportTenants }) => {
  const [search, setSearch] = useState('');
  const [showAddForm, setShowAddForm] = useState(false);
  const [newName, setNewName] = useState('');
  const [newPhone, setNewPhone] = useState('');
  const [newNotes, setNewNotes] = useState('');
  const [importStatus, setImportStatus] = useState<string | null>(null);

  const filteredContacts = contacts.filter((c) =>
    c.full_name.toLowerCase().includes(search.toLowerCase()) ||
    c.phone_raw.includes(search) ||
    c.phone_e164.includes(search) ||
    c.notes.toLowerCase().includes(search.toLowerCase())
  );

  const handleManualAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newPhone.trim()) return;

    await onImportTenants([
      {
        name: newName.trim() || 'New Tenant',
        phone: newPhone.trim(),
        notes: newNotes.trim() || 'Manual dashboard entry',
      },
    ]);

    setNewName('');
    setNewPhone('');
    setNewNotes('');
    setShowAddForm(false);
    setImportStatus('Successfully added 1 tenant contact.');
    setTimeout(() => setImportStatus(null), 4000);
  };

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = async (event) => {
      const text = event.target?.result as string;
      if (!text) return;

      const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
      const parsedRows: Array<{ name: string; phone: string; notes: string }> = [];

      // Assume CSV with headers (Name, Phone, Notes) or simple lines
      const startIndex = lines[0].toLowerCase().includes('phone') ? 1 : 0;
      for (let i = startIndex; i < lines.length; i++) {
        const parts = lines[i].split(',').map((p) => p.replace(/^["']|["']$/g, '').trim());
        if (parts.length >= 2) {
          parsedRows.push({
            name: parts[0],
            phone: parts[1],
            notes: parts[2] || `Imported from ${file.name}`,
          });
        } else if (parts[0]) {
          parsedRows.push({
            name: 'Imported Candidate',
            phone: parts[0],
            notes: `Imported from ${file.name}`,
          });
        }
      }

      if (parsedRows.length > 0) {
        await onImportTenants(parsedRows);
        setImportStatus(`Successfully parsed and imported ${parsedRows.length} tenant contacts from ${file.name}!`);
        setTimeout(() => setImportStatus(null), 5000);
      } else {
        setImportStatus('No valid rows found in the uploaded file.');
      }
    };
    reader.readAsText(file);
  };

  return (
    <div className="space-y-4">
      {/* Top action bar */}
      <div className="flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3 bg-slate-800/40 p-3.5 rounded-xl border border-slate-700/60">
        <div className="relative flex-1 max-w-md">
          <Search className="absolute left-3 top-2.5 w-4 h-4 text-slate-400" />
          <input
            id="tenant-search-input"
            type="text"
            placeholder="Search tenant name, phone, preferences..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-9 pr-3 py-2 bg-slate-900/90 border border-slate-700 rounded-lg text-sm text-slate-100 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-amber-500"
          />
        </div>

        <div className="flex items-center space-x-2">
          <label
            htmlFor="tenant-csv-upload"
            className="inline-flex items-center space-x-2 px-3 py-2 bg-slate-700 hover:bg-slate-600 border border-slate-600 rounded-lg text-xs font-medium text-slate-200 cursor-pointer transition shadow-sm"
          >
            <Upload className="w-3.5 h-3.5 text-amber-400" />
            <span>Import CSV</span>
            <input
              id="tenant-csv-upload"
              type="file"
              accept=".csv"
              className="hidden"
              onChange={handleFileUpload}
            />
          </label>

          <button
            id="add-tenant-toggle-btn"
            onClick={() => setShowAddForm(!showAddForm)}
            className="inline-flex items-center space-x-2 px-3.5 py-2 bg-amber-600 hover:bg-amber-500 text-white rounded-lg text-xs font-semibold shadow-sm transition"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>{showAddForm ? 'Cancel' : 'Add Contact'}</span>
          </button>
        </div>
      </div>

      {importStatus && (
        <div className="p-3 bg-emerald-950/60 border border-emerald-800 rounded-lg text-emerald-200 text-xs flex items-center space-x-2">
          <CheckCircle className="w-4 h-4 text-emerald-400 shrink-0" />
          <span>{importStatus}</span>
        </div>
      )}

      {/* Add form modal / drawer */}
      {showAddForm && (
        <form onSubmit={handleManualAdd} className="bg-slate-800/80 border border-slate-700 p-4 rounded-xl space-y-3">
          <h4 className="text-xs font-bold text-white uppercase tracking-wider">Add Tenant Candidate</h4>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            <div>
              <label className="block text-[11px] text-slate-400 mb-1">Full Name</label>
              <input
                type="text"
                placeholder="e.g. Георги Иванов"
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                className="w-full px-3 py-1.5 bg-slate-900 border border-slate-700 rounded text-xs text-white"
              />
            </div>
            <div>
              <label className="block text-[11px] text-slate-400 mb-1">Phone Number (BG format)</label>
              <input
                type="text"
                required
                placeholder="e.g. 0888 123 456"
                value={newPhone}
                onChange={(e) => setNewPhone(e.target.value)}
                className="w-full px-3 py-1.5 bg-slate-900 border border-slate-700 rounded text-xs text-white font-mono"
              />
            </div>
            <div>
              <label className="block text-[11px] text-slate-400 mb-1">Search Notes / Criteria</label>
              <input
                type="text"
                placeholder="e.g. Търси 2-стаен до 600 евро"
                value={newNotes}
                onChange={(e) => setNewNotes(e.target.value)}
                className="w-full px-3 py-1.5 bg-slate-900 border border-slate-700 rounded text-xs text-white"
              />
            </div>
          </div>
          <div className="flex justify-end pt-2">
            <button
              type="submit"
              className="px-4 py-1.5 bg-amber-600 hover:bg-amber-500 text-white rounded text-xs font-semibold"
            >
              Save Tenant
            </button>
          </div>
        </form>
      )}

      {/* Tenant Table */}
      <div className="bg-slate-800/50 border border-slate-700/60 rounded-xl overflow-hidden shadow-sm">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-900/80 text-slate-400 border-b border-slate-700/80 uppercase font-semibold">
              <tr>
                <th className="py-3 px-3 w-12">#</th>
                <th className="py-3 px-3">Name</th>
                <th className="py-3 px-3">Normalized Phone</th>
                <th className="py-3 px-3">E.164 Format</th>
                <th className="py-3 px-3">Requirement Notes</th>
                <th className="py-3 px-3">Source</th>
                <th className="py-3 px-3">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {filteredContacts.length === 0 ? (
                <tr>
                  <td colSpan={7} className="py-8 text-center text-slate-400">
                    No tenant contacts registered yet.
                  </td>
                </tr>
              ) : (
                filteredContacts.map((contact, idx) => (
                  <tr key={contact.id || idx} className="hover:bg-slate-700/30 text-slate-300 transition">
                    <td className="py-3 px-3 text-slate-500 font-mono">{idx + 1}</td>
                    <td className="py-3 px-3 font-semibold text-white">{contact.full_name}</td>
                    <td className="py-3 px-3 font-mono text-slate-300">{contact.phone_normalized}</td>
                    <td className="py-3 px-3 font-mono text-emerald-400">{contact.phone_e164}</td>
                    <td className="py-3 px-3 text-slate-300 max-w-sm">{contact.notes || '-'}</td>
                    <td className="py-3 px-3">
                      <span className="px-2 py-0.5 rounded bg-slate-900 text-slate-400 border border-slate-700 text-[10px]">
                        {contact.import_source}
                      </span>
                    </td>
                    <td className="py-3 px-3">
                      <span className="px-2 py-0.5 rounded-full text-[10px] bg-emerald-950 text-emerald-300 border border-emerald-800 font-medium">
                        Active
                      </span>
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
