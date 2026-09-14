export type LeadStatus = 'New' | 'Qualified' | 'Contacted' | 'Pending';

export interface Lead {
  ad_id: string;
  date_seen: string;
  title: string;
  price: string;
  location: string;
  size: string;
  link: string;
  source_site: string;
  phone: string;
  seller_name: string;
  ad_type: string;
  contact_name: string;
  contact_email: string;
  status?: LeadStatus;
  updated_at?: string;
}

export interface Agency {
  id: number;
  agency_name: string;
  phones: string;
  city: string;
  email: string;
  contact_name: string;
  profile_url?: string;
  updated_at?: string;
}

export interface VoiceCall {
  id: number;
  source_type: string;
  listing_ad_id?: string | null;
  tenant_contact_id?: number | null;
  twilio_call_sid?: string | null;
  contact_name: string;
  phone_raw: string;
  phone_e164: string;
  status: 'queued' | 'in-progress' | 'completed' | 'failed' | 'no-answer' | 'busy';
  script_name: string;
  answers_json: Record<string, string>;
  transcript?: string | null;
  recording_url?: string | null;
  last_error?: string | null;
  initiated_by: string;
  started_at?: string | null;
  answered_at?: string | null;
  completed_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  listing_title?: string | null;
  listing_link?: string | null;
}

export interface TenantContact {
  id: number;
  full_name: string;
  phone_raw: string;
  phone_normalized: string;
  phone_e164: string;
  notes: string;
  import_source: string;
  active: boolean;
  created_at?: string;
  updated_at?: string;
}

export interface ScraperStats {
  worker_status: 'idle' | 'running' | 'scheduled';
  last_status: 'ok' | 'running' | 'error' | 'idle';
  last_started_at: string | null;
  last_finished_at: string | null;
  last_total_scraped: number;
  last_written: number;
  last_error: string | null;
  active_sources: string[];
}

export interface TriggerScrapeResponse {
  status: string;
  message: string;
  task_id?: string;
}

export interface HealthStatus {
  status: string;
  app: string;
  redis: string;
  database: string;
  uptime: number;
  timestamp: string;
}
