import { Lead, Agency, VoiceCall, TenantContact, ScraperStats } from '../types';

export const initialLeads: Lead[] = [
  {
    ad_id: "imoti-109283",
    date_seen: new Date().toISOString().split('T')[0],
    title: "2-стаен апартамент в Лозенец, напълно обзаведен",
    price: "750 EUR / мес.",
    location: "София, Лозенец",
    size: "68 кв.м",
    link: "https://imoti.bg/obiava/109283",
    source_site: "imoti.bg",
    phone: "0888492790",
    seller_name: "ЕсЕл&ДжиЕр Къмпани",
    ad_type: "Под наем",
    contact_name: "Силвия Георгиева",
    contact_email: "silvia@slgr.bg",
    status: "Qualified",
    updated_at: new Date().toISOString()
  },
  {
    ad_id: "imoti-109284",
    date_seen: new Date().toISOString().split('T')[0],
    title: "3-стаен луксозен апартамент до Южен парк",
    price: "1 200 EUR / мес.",
    location: "София, Иван Вазов",
    size: "115 кв.м",
    link: "https://imoti.bg/obiava/109284",
    source_site: "imoti.bg",
    phone: "0896380248",
    seller_name: "Империум Груп 2020",
    ad_type: "Под наем",
    contact_name: "Димитър Николов",
    contact_email: "d.nikolov@imperium.bg",
    status: "Contacted",
    updated_at: new Date().toISOString()
  },
  {
    ad_id: "alo-847291",
    date_seen: new Date().toISOString().split('T')[0],
    title: "Студио под наем в центъра на Варна до ВИНС",
    price: "450 BGN / мес.",
    location: "Варна, Център",
    size: "38 кв.м",
    link: "https://alo.bg/obiava/847291",
    source_site: "alo.bg",
    phone: "0878916209",
    seller_name: "МАТЕНА ЕСТЕЙТ",
    ad_type: "Под наем",
    contact_name: "Мария Иванова",
    contact_email: "office@matena.bg",
    status: "Qualified",
    updated_at: new Date().toISOString()
  },
  {
    ad_id: "alo-847292",
    date_seen: new Date().toISOString().split('T')[0],
    title: "1-стаен реновиран апартамент до Метростанция Сердика",
    price: "480 EUR / мес.",
    location: "София, Център",
    size: "45 кв.м",
    link: "https://alo.bg/obiava/847292",
    source_site: "alo.bg",
    phone: "0887421661",
    seller_name: "Про Пак ООД",
    ad_type: "Под наем",
    contact_name: "Петър Василев",
    contact_email: "propak@estate.bg",
    status: "New",
    updated_at: new Date().toISOString()
  },
  {
    ad_id: "domria-938201",
    date_seen: new Date().toISOString().split('T')[0],
    title: "2-кімнатна квартира з видом на Дніпро, Печерськ",
    price: "850 USD / міс.",
    location: "Київ, Печерський район",
    size: "72 кв.м",
    link: "https://dom.ria.com/uk/realty-938201",
    source_site: "dom.ria.com",
    phone: "+380671234567",
    seller_name: "Park Lane",
    ad_type: "Оренда",
    contact_name: "Оксана Мельник",
    contact_email: "oksana@parklane.ua",
    status: "New",
    updated_at: new Date().toISOString()
  },
  {
    ad_id: "olx-773829",
    date_seen: new Date().toISOString().split('T')[0],
    title: "Затишна 1к квартира біля КПІ з кондиціонером",
    price: "14 000 UAH / міс.",
    location: "Київ, Солом'янський район",
    size: "40 кв.м",
    link: "https://olx.ua/d/uk/obyavlenie/773829",
    source_site: "olx.ua",
    phone: "+380509876543",
    seller_name: "Власник",
    ad_type: "Оренда",
    contact_name: "Андрій Коваленко",
    contact_email: "andriy.k@gmail.com",
    status: "Contacted",
    updated_at: new Date().toISOString()
  },
  {
    ad_id: "lun-554210",
    date_seen: new Date().toISOString().split('T')[0],
    title: "Апартаменти в ЖК Файна Таун, новий ремонт",
    price: "22 000 UAH / міс.",
    location: "Київ, Нивки",
    size: "52 кв.м",
    link: "https://lun.ua/rent/554210",
    source_site: "lun.ua",
    phone: "+380634567890",
    seller_name: "Kovcheg Real Estate",
    ad_type: "Оренда",
    contact_name: "Ірина Савченко",
    contact_email: "isavchenko@kovcheg.com",
    status: "New",
    updated_at: new Date().toISOString()
  }
];

export const initialAgencies: Agency[] = [
  {
    id: 1,
    agency_name: "ЕсЕл&ДжиЕр Къмпани ЕООД",
    phones: "0888492790",
    city: "София",
    email: "office@slgr.bg",
    contact_name: "Силвия Георгиева",
    profile_url: "https://imoti.bg/обяви/agu:2844",
    updated_at: new Date().toISOString()
  },
  {
    id: 2,
    agency_name: "Империум Груп 2020 ООД",
    phones: "+359896380248",
    city: "София",
    email: "office@imperium.bg",
    contact_name: "Димитър Николов",
    profile_url: "https://imoti.bg/обяви/agu:3098",
    updated_at: new Date().toISOString()
  },
  {
    id: 3,
    agency_name: "МАТЕНА ЕСТЕЙТ ЕООД",
    phones: "0878916209",
    city: "Варна",
    email: "office@matena.bg",
    contact_name: "Мария Иванова",
    profile_url: "https://imoti.bg/обяви/agu:3497",
    updated_at: new Date().toISOString()
  },
  {
    id: 4,
    agency_name: "Про Пак ООД",
    phones: "0887421661",
    city: "София",
    email: "propak@estate.bg",
    contact_name: "Петър Василев",
    profile_url: "https://imoti.bg/обяви/agu:29450",
    updated_at: new Date().toISOString()
  },
  {
    id: 5,
    agency_name: "366ESTATE",
    phones: "0888933666",
    city: "Пловдив",
    email: "info@366estate.bg",
    contact_name: "Калоян Тодоров",
    profile_url: "https://imoti.bg/обяви/agu:4185",
    updated_at: new Date().toISOString()
  },
  {
    id: 6,
    agency_name: "Albertin Real Estate ltd.",
    phones: "+359897637767",
    city: "Бургас",
    email: "office@albertin.bg",
    contact_name: "Алберт Илиев",
    profile_url: "https://imoti.bg/обяви/agu:28191",
    updated_at: new Date().toISOString()
  },
  {
    id: 7,
    agency_name: "Atlanta Real Estate",
    phones: "+359884551510",
    city: "Варна",
    email: "office@atlanta.bg",
    contact_name: "Атанас Михайлов",
    profile_url: "https://imoti.bg/обяви/agu:3184",
    updated_at: new Date().toISOString()
  },
  {
    id: 8,
    agency_name: "BG Home Services Ltd",
    phones: "0885492310",
    city: "София",
    email: "support@bghome.bg",
    contact_name: "Елена Попова",
    profile_url: "https://imoti.bg/обяви/agu:5489",
    updated_at: new Date().toISOString()
  }
];

export const initialVoiceCalls: VoiceCall[] = [
  {
    id: 1,
    source_type: "listing",
    listing_ad_id: "imoti-109283",
    listing_title: "2-стаен апартамент в Лозенец, напълно обзаведен",
    listing_link: "https://imoti.bg/obiava/109283",
    contact_name: "Силвия Георгиева",
    phone_raw: "0888492790",
    phone_e164: "+359888492790",
    status: "completed",
    script_name: "bg_listing_qualification_v1",
    answers_json: {
      "availability": "Свободен за огледи от утре",
      "price_negotiable": "Възможен коментар при дългосрочен договор",
      "pets_allowed": "Да, за малки домашни любимци с двоен депозит",
      "heating": "ТЕЦ и инверторен климатик"
    },
    transcript: "Асистент: Здравейте, обаждам се относно обявата за двустаен в Лозенец. Имотът свободен ли е за наемане?\nБрокер: Здравейте, да, апартаментът е свободен, можем да организираме оглед утре след 14:00 часа.\nАсистент: Цената от 750 евро подлежи ли на коментар?\nБрокер: При наемане за период над 2 години можем да обсъдим малка отстъпка.\nАсистент: Приемат ли се домашни любимци?\nБрокер: Само малки породи кучета с двоен гаранционен депозит.\nАсистент: Благодаря Ви, записах детайлите. Приятен ден!",
    recording_url: "https://api.twilio.com/mock-recordings/rec_0182749a.mp3",
    initiated_by: "Pavlo (admin)",
    started_at: "2026-09-14T09:15:00Z",
    answered_at: "2026-09-14T09:15:05Z",
    completed_at: "2026-09-14T09:16:32Z",
    created_at: "2026-09-14T09:14:58Z",
    updated_at: "2026-09-14T09:16:35Z"
  },
  {
    id: 2,
    source_type: "listing",
    listing_ad_id: "alo-847291",
    listing_title: "Студио под наем в центъра на Варна до ВИНС",
    listing_link: "https://alo.bg/obiava/847291",
    contact_name: "Мария Иванова",
    phone_raw: "0878916209",
    phone_e164: "+359878916209",
    status: "completed",
    script_name: "bg_listing_qualification_v1",
    answers_json: {
      "availability": "Свободен веднага",
      "price_negotiable": "Крайна цена",
      "heating": "Климатик"
    },
    transcript: "Асистент: Здравейте, обаждам се за студиото до ВИНС във Варна. Свободно ли е веднага?\nБрокер: Да, готово е за нанасяне от днес.\nАсистент: Цената 450 лв твърда ли е?\nБрокер: Да, това е крайна цена с включен интернет.\nАсистент: Чудесно, благодаря Ви!",
    recording_url: "https://api.twilio.com/mock-recordings/rec_0182751b.mp3",
    initiated_by: "Pavlo (admin)",
    started_at: "2026-09-14T10:02:10Z",
    answered_at: "2026-09-14T10:02:14Z",
    completed_at: "2026-09-14T10:03:20Z",
    created_at: "2026-09-14T10:02:08Z",
    updated_at: "2026-09-14T10:03:22Z"
  },
  {
    id: 3,
    source_type: "listing",
    listing_ad_id: "imoti-109284",
    listing_title: "3-стаен луксозен апартамент до Южен парк",
    listing_link: "https://imoti.bg/obiava/109284",
    contact_name: "Димитър Николов",
    phone_raw: "0896380248",
    phone_e164: "+359896380248",
    status: "no-answer",
    script_name: "bg_listing_qualification_v1",
    answers_json: {},
    transcript: null,
    last_error: "No answer after 30 seconds ring timeout",
    initiated_by: "Pavlo (admin)",
    started_at: "2026-09-14T11:10:00Z",
    answered_at: null,
    completed_at: "2026-09-14T11:10:35Z",
    created_at: "2026-09-14T11:09:55Z",
    updated_at: "2026-09-14T11:10:36Z"
  }
];

export const initialTenantContacts: TenantContact[] = [
  {
    id: 1,
    full_name: "Борислав Стоянов",
    phone_raw: "0888 123 456",
    phone_normalized: "0888123456",
    phone_e164: "+359888123456",
    notes: "Търси 2-стаен в Лозенец или Център, бюджет до 800 EUR",
    import_source: "manual_entry",
    active: true,
    created_at: "2026-09-14T08:30:00Z",
    updated_at: "2026-09-14T08:30:00Z"
  },
  {
    id: 2,
    full_name: "Гергана Петрова",
    phone_raw: "0899 987 654",
    phone_normalized: "0899987654",
    phone_e164: "+359899987654",
    notes: "Търси студио или 1-стаен във Варна до университет",
    import_source: "csv_import_tenants_sep.csv",
    active: true,
    created_at: "2026-09-14T09:00:00Z",
    updated_at: "2026-09-14T09:00:00Z"
  },
  {
    id: 3,
    full_name: "Пламен Димитров",
    phone_raw: "0877 345 678",
    phone_normalized: "0877345678",
    phone_e164: "+359877345678",
    notes: "Семейство с дете, 3-стаен около Южен или Западен парк",
    import_source: "csv_import_tenants_sep.csv",
    active: true,
    created_at: "2026-09-14T09:00:00Z",
    updated_at: "2026-09-14T09:00:00Z"
  }
];

export const initialScraperStats: ScraperStats = {
  worker_status: 'idle',
  last_status: 'ok',
  last_started_at: "2026-09-14T11:00:00Z",
  last_finished_at: "2026-09-14T11:02:15Z",
  last_total_scraped: 48,
  last_written: 12,
  last_error: null,
  active_sources: ['imoti.bg', 'alo.bg', 'dom.ria.com', 'olx.ua', 'lun.ua']
};
