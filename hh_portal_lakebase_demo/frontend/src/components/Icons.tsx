// Inline line icons (decorative; aria-hidden). Generic shapes, no brand marks.
import type { ReactNode } from "react";

function Svg({ children, size = 18 }: { children: ReactNode; size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.6}
      strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false">{children}</svg>
  );
}

export const IconOverview = () => <Svg><rect x="3" y="5" width="18" height="14" rx="2" /><path d="M3 10h18M8 15h3" /></Svg>;
export const IconMeter = () => <Svg><rect x="3" y="4" width="18" height="16" rx="2" /><path d="M7 9h10M7 13h3m3 0h4M7 17h10" /></Svg>;
export const IconInvoice = () => <Svg><path d="M7 3h10v18l-3-2-2 2-2-2-3 2z" /><path d="M10 8h4M10 12h4" /></Svg>;
export const IconReport = () => <Svg><path d="M4 20V10M10 20V4M16 20v-7M22 20H2" /></Svg>;
export const IconContract = () => <Svg><path d="M6 3h9l4 4v14H6z" /><path d="M14 3v5h5M9 13h7M9 17h5" /></Svg>;
export const IconHalfHourly = () => <Svg><path d="M5 20v-6M9 20V9M13 20v-9M17 20V6M21 20H3" /></Svg>;
export const IconUser = ({ size = 18 }: { size?: number }) => <Svg size={size}><circle cx="12" cy="8" r="3.5" /><path d="M5 20c1.2-3.5 4-5 7-5s5.8 1.5 7 5" /></Svg>;
export const IconBuilding = () => <Svg><path d="M4 21V8l8-5 8 5v13M9 21v-5h6v5M8 11h2m4 0h2" /></Svg>;
export const IconFilter = () => <Svg size={16}><path d="M4 6h16M7 12h10M10 18h4" /></Svg>;
export const IconChevronDown = () => <Svg size={16}><path d="m6 9 6 6 6-6" /></Svg>;
export const IconChevronUp = () => <Svg size={16}><path d="m6 15 6-6 6 6" /></Svg>;
export const IconChevronLeft = () => <Svg size={16}><path d="m15 6-6 6 6 6" /></Svg>;
export const IconChevronRight = () => <Svg size={16}><path d="m9 6 6 6-6 6" /></Svg>;
export const IconArrowCircle = () => <Svg size={18}><circle cx="12" cy="12" r="9" /><path d="m11 8 4 4-4 4" /></Svg>;
export const IconSearch = () => <Svg size={16}><circle cx="11" cy="11" r="6" /><path d="m20 20-4.5-4.5" /></Svg>;
export const IconClose = () => <Svg size={16}><path d="M6 6l12 12M18 6 6 18" /></Svg>;
export const IconBolt = () => <Svg size={18}><circle cx="12" cy="12" r="9" /><path d="m13 6-4 7h3l-1 5 4-7h-3z" /></Svg>;
export const IconInfo = () => <Svg size={18}><circle cx="12" cy="12" r="9" /><path d="M12 11v5M12 8h.01" /></Svg>;
export const IconChart = () => <Svg size={20}><circle cx="12" cy="12" r="9.5" /><path d="m7 15 3-3 2 2 5-5" /></Svg>;
export const IconSummary = () => <Svg size={18}><rect x="3" y="5" width="18" height="14" rx="4" /><path d="M8 11h.01M16 11h.01M9 15c1.8 1.2 4.2 1.2 6 0" /></Svg>;
export const IconUp = () => <Svg size={14}><path d="m6 15 6-6 6 6" /></Svg>;
export const IconDown = () => <Svg size={14}><path d="m6 9 6 6 6-6" /></Svg>;
export const IconMenu = () => <Svg size={20}><path d="M4 7h16M4 12h16M4 17h16" /></Svg>;
export const IconArchitecture = () => <Svg><rect x="2.5" y="9" width="5" height="6" rx="1" /><rect x="9.5" y="4" width="5" height="6" rx="1" /><rect x="9.5" y="14" width="5" height="6" rx="1" /><rect x="16.5" y="9" width="5" height="6" rx="1" /><path d="M7.5 12h2m5-5h2v5m-2 5h2v-5" /></Svg>;
export const IconExternal = () => <Svg size={14}><path d="M14 4h6v6M20 4l-9 9M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5" /></Svg>;
