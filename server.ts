import express, { Request, Response } from "express";
import cors from "cors";
import path from "path";
import { createServer as createViteServer } from "vite";
import {
  initialLeads,
  initialAgencies,
  initialVoiceCalls,
  initialTenantContacts,
  initialScraperStats,
} from "./src/data/initialData";
import { Lead, Agency, VoiceCall, TenantContact, ScraperStats } from "./src/types";

async function startServer() {
  const app = express();
  const PORT = 3000;

  app.use(cors());
  app.use(express.json({ limit: "10mb" }));

  // In-memory data stores
  let leads: Lead[] = [...initialLeads];
  let agencies: Agency[] = [...initialAgencies];
  let voiceCalls: VoiceCall[] = [...initialVoiceCalls];
  let tenantContacts: TenantContact[] = [...initialTenantContacts];
  let scraperStats: ScraperStats = { ...initialScraperStats };
  const startTime = Date.now();

  // --- API Endpoints ---

  // Health endpoint
  const handleHealth = (req: Request, res: Response) => {
    res.json({
      status: "ok",
      app: "Real Estate SaaS Core",
      redis: "ok (in-memory)",
      database: "ok (in-memory)",
      uptime: Math.floor((Date.now() - startTime) / 1000),
      timestamp: new Date().toISOString(),
    });
  };
  app.get("/health", handleHealth);
  app.get("/api/health", handleHealth);

  // Leads endpoint
  const handleGetLeads = (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || "100", 10), 1000);
    const search = ((req.query.search as string) || "").toLowerCase();
    const source = (req.query.source as string) || "";

    let filtered = leads;
    if (search) {
      filtered = filtered.filter(
        (l) =>
          l.title.toLowerCase().includes(search) ||
          l.location.toLowerCase().includes(search) ||
          l.seller_name.toLowerCase().includes(search) ||
          l.phone.includes(search)
      );
    }
    if (source && source !== "all") {
      filtered = filtered.filter((l) => l.source_site === source);
    }

    res.json(filtered.slice(0, limit));
  };
  app.get("/leads", handleGetLeads);
  app.get("/api/leads", handleGetLeads);

  // Update lead status
  const handleUpdateLeadStatus = (req: Request, res: Response) => {
    const { id } = req.params;
    const { status } = req.body;
    const targetLead = leads.find((l) => l.ad_id === id);
    if (!targetLead) {
      return res.status(404).json({ error: "Lead not found" });
    }
    targetLead.status = status;
    targetLead.updated_at = new Date().toISOString();
    res.json(targetLead);
  };
  app.patch("/leads/:id/status", handleUpdateLeadStatus);
  app.patch("/api/leads/:id/status", handleUpdateLeadStatus);

  // Agencies endpoint
  const handleGetAgencies = (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || "100", 10), 1000);
    const search = ((req.query.search as string) || "").toLowerCase();
    const city = (req.query.city as string) || "";

    let filtered = agencies;
    if (search) {
      filtered = filtered.filter(
        (a) =>
          a.agency_name.toLowerCase().includes(search) ||
          a.phones.includes(search) ||
          a.contact_name.toLowerCase().includes(search)
      );
    }
    if (city && city !== "all") {
      filtered = filtered.filter((a) => a.city === city);
    }

    res.json(filtered.slice(0, limit));
  };
  app.get("/agencies", handleGetAgencies);
  app.get("/api/agencies", handleGetAgencies);

  // Scrape Trigger endpoint
  const handleTriggerScrape = (req: Request, res: Response) => {
    scraperStats.worker_status = "running";
    scraperStats.last_status = "running";
    scraperStats.last_started_at = new Date().toISOString();
    scraperStats.last_error = null;

    // Simulate async scraper completion after 3 seconds
    setTimeout(() => {
      const newLeadId = `imoti-${Math.floor(100000 + Math.random() * 900000)}`;
      const simulatedLead: Lead = {
        ad_id: newLeadId,
        date_seen: new Date().toISOString().split("T")[0],
        title: `Нова обява: 2-стаен апартамент в кв. Младост`,
        price: "600 EUR / мес.",
        location: "София, Младост 1",
        size: "65 кв.м",
        link: `https://imoti.bg/obiava/${newLeadId}`,
        source_site: "imoti.bg",
        phone: "0888999888",
        seller_name: "Частно лице",
        ad_type: "Под наем",
        contact_name: "Иван Тодоров",
        contact_email: "ivan.t@example.com",
        status: "New",
        updated_at: new Date().toISOString(),
      };
      leads.unshift(simulatedLead);

      scraperStats.worker_status = "idle";
      scraperStats.last_status = "ok";
      scraperStats.last_finished_at = new Date().toISOString();
      scraperStats.last_total_scraped = leads.length;
      scraperStats.last_written += 1;
    }, 3000);

    res.json({
      status: "queued",
      message: "Scrape task has been queued and is executing in background worker.",
      task_id: `task_${Date.now()}`,
    });
  };
  app.post("/trigger-scrape", handleTriggerScrape);
  app.post("/api/trigger-scrape", handleTriggerScrape);

  // Voice Calls endpoints
  const handleGetVoiceCalls = (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || "100", 10), 1000);
    res.json(voiceCalls.slice(0, limit));
  };
  app.get("/voice/calls", handleGetVoiceCalls);
  app.get("/api/voice/calls", handleGetVoiceCalls);

  const handleCreateVoiceCall = (req: Request, res: Response) => {
    const { listing_ad_id, initiated_by } = req.body;
    const targetLead = leads.find((l) => l.ad_id === listing_ad_id);

    const callId = voiceCalls.length + 1;
    const newCall: VoiceCall = {
      id: callId,
      source_type: "listing",
      listing_ad_id: listing_ad_id || "direct",
      listing_title: targetLead ? targetLead.title : "Direct Contact Call",
      listing_link: targetLead ? targetLead.link : null,
      contact_name: targetLead ? targetLead.contact_name || targetLead.seller_name : "Broker",
      phone_raw: targetLead ? targetLead.phone : "+359888123456",
      phone_e164: targetLead && targetLead.phone.startsWith("+")
        ? targetLead.phone
        : `+359${(targetLead?.phone || "888123456").replace(/^0/, "")}`,
      status: "in-progress",
      script_name: "bg_listing_qualification_v1",
      answers_json: {},
      transcript: null,
      recording_url: null,
      initiated_by: initiated_by || "api",
      started_at: new Date().toISOString(),
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };

    voiceCalls.unshift(newCall);

    if (targetLead) {
      targetLead.status = "Contacted";
      targetLead.updated_at = new Date().toISOString();
    }

    // Simulate bot completing call with structured results after 4 seconds
    setTimeout(() => {
      newCall.status = "completed";
      newCall.answered_at = new Date().toISOString();
      newCall.completed_at = new Date().toISOString();
      newCall.answers_json = {
        availability: "Потвърдено: свободен за наемане веднага",
        price_negotiable: "Възможен коментар при 12-месечен предплатен наем",
        pets: "Позволени малки кучета",
      };
      newCall.transcript = `Асистент: Здравейте, обаждам се относно обява #${listing_ad_id}. Свободен ли е имотът?\nБрокер: Да, свободен е веднага, огледи всеки делничен ден.\nАсистент: Има ли възможност за коментар на наема?\nБрокер: Да, при сериозен интерес и бърз договор.\nАсистент: Благодаря, информацията е записана!`;
      newCall.recording_url = `https://api.twilio.com/mock-recordings/rec_${callId}.mp3`;
      newCall.updated_at = new Date().toISOString();

      if (targetLead) {
        targetLead.status = "Qualified";
        targetLead.updated_at = new Date().toISOString();
      }
    }, 4000);

    res.status(201).json(newCall);
  };
  app.post("/voice/calls", handleCreateVoiceCall);
  app.post("/api/voice/calls", handleCreateVoiceCall);

  // Tenant Contacts endpoints
  const handleGetTenantContacts = (req: Request, res: Response) => {
    const limit = Math.min(parseInt((req.query.limit as string) || "100", 10), 1000);
    res.json(tenantContacts.slice(0, limit));
  };
  app.get("/voice/tenants", handleGetTenantContacts);
  app.get("/api/voice/tenants", handleGetTenantContacts);

  const handleImportTenants = (req: Request, res: Response) => {
    const rows = req.body;
    if (!Array.isArray(rows)) {
      return res.status(400).json({ error: "Expected an array of tenant contact rows" });
    }

    let imported = 0;
    for (const row of rows) {
      const rawPhone = String(row.phone_raw || row.phone || "").trim();
      if (!rawPhone) continue;
      const normalized = rawPhone.replace(/\D/g, "");
      const e164 = rawPhone.startsWith("+") ? rawPhone : `+359${normalized.replace(/^0/, "")}`;

      const newContact: TenantContact = {
        id: tenantContacts.length + 1,
        full_name: row.full_name || row.name || "Tenant Candidate",
        phone_raw: rawPhone,
        phone_normalized: normalized,
        phone_e164: e164,
        notes: row.notes || "Imported via web dashboard",
        import_source: row.import_source || "dashboard_csv_import",
        active: true,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      tenantContacts.unshift(newContact);
      imported++;
    }

    res.json({ imported, skipped: rows.length - imported });
  };
  app.post("/voice/tenants/import", handleImportTenants);
  app.post("/api/voice/tenants/import", handleImportTenants);

  // Scraper Stats
  const handleGetScraperStats = (req: Request, res: Response) => {
    scraperStats.last_total_scraped = leads.length;
    res.json(scraperStats);
  };
  app.get("/scraper/stats", handleGetScraperStats);
  app.get("/api/scraper/stats", handleGetScraperStats);

  // --- Vite Frontend Integration ---
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Real Estate SaaS Core running on http://0.0.0.0:${PORT}`);
  });
}

startServer().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
