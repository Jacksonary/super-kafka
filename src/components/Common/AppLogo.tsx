import React from "react";

interface AppLogoProps {
  size?: number;
  className?: string;
  style?: React.CSSProperties;
}

/**
 * Super Kafka 主品牌 Logo 组件 (方案 A: Hyper-Stream K)
 *
 * 设计特性：
 * 1. 采用纯 SVG 矢量渲染，高分屏绝对锐利无锯齿，零运行时开销。
 * 2. 深度绑定 `html[data-theme]` 属性，借助 CSS 选择器与 CSS 变量实现第 0 帧即时换肤（Anti-FOUC）。
 * 3. 深色模式 (Dark)：黑曜石深空底座 + 赛博电光青与紫绀能量流，极具科技穿透力；
 * 4. 浅色模式 (Light)：高透晶体底座 + 高对比度深海深青与极速靛蓝，清爽规整，对比度达标。
 */
export const AppLogo: React.FC<AppLogoProps> = ({
  size = 32,
  className,
  style,
}) => {
  return (
    <svg
      viewBox="0 0 512 512"
      width={size}
      height={size}
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      style={{
        display: "block",
        flexShrink: 0,
        ...style,
      }}
    >
      <defs>
        {/* ── 深色模式渐变定义 ── */}
        <linearGradient id="skDarkBg" x1="47" y1="47" x2="465" y2="465" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#151C2C" />
          <stop offset="50%" stopColor="#0C101A" />
          <stop offset="100%" stopColor="#05070B" />
        </linearGradient>
        <linearGradient id="skDarkRim" x1="47" y1="47" x2="465" y2="465" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#38BDF8" stopOpacity="0.6" />
          <stop offset="35%" stopColor="#818CF8" stopOpacity="0.3" />
          <stop offset="100%" stopColor="#1E293B" stopOpacity="0.3" />
        </linearGradient>
        <linearGradient id="skDarkLog" x1="145" y1="130" x2="190" y2="380" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#38BDF8" />
          <stop offset="50%" stopColor="#0284C7" />
          <stop offset="100%" stopColor="#0369A1" />
        </linearGradient>
        <linearGradient id="skDarkBeam" x1="215" y1="130" x2="375" y2="230" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#38BDF8" />
          <stop offset="60%" stopColor="#818CF8" />
          <stop offset="100%" stopColor="#C084FC" />
        </linearGradient>
        <linearGradient id="skDarkStream" x1="247" y1="266" x2="380" y2="380" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#00F2FE" />
          <stop offset="45%" stopColor="#38BDF8" />
          <stop offset="100%" stopColor="#6366F1" />
        </linearGradient>

        {/* ── 浅色模式渐变定义 (高对比度清爽质感) ── */}
        <linearGradient id="skLightBg" x1="47" y1="47" x2="465" y2="465" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#FFFFFF" />
          <stop offset="100%" stopColor="#F1F5F9" />
        </linearGradient>
        <linearGradient id="skLightRim" x1="47" y1="47" x2="465" y2="465" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#0284C7" stopOpacity="0.3" />
          <stop offset="100%" stopColor="#CBD5E1" />
        </linearGradient>
        <linearGradient id="skLightLog" x1="145" y1="130" x2="190" y2="380" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#0284C7" />
          <stop offset="50%" stopColor="#0369A1" />
          <stop offset="100%" stopColor="#0F172A" />
        </linearGradient>
        <linearGradient id="skLightBeam" x1="215" y1="130" x2="375" y2="230" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#0284C7" />
          <stop offset="60%" stopColor="#6366F1" />
          <stop offset="100%" stopColor="#7C3AED" />
        </linearGradient>
        <linearGradient id="skLightStream" x1="247" y1="266" x2="380" y2="380" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#0284C7" />
          <stop offset="55%" stopColor="#4F46E5" />
          <stop offset="100%" stopColor="#1E1B4B" />
        </linearGradient>
      </defs>

      {/* ── CSS 驱动自适应样式 (防闪烁) ── */}
      <style>{`
        /* 默认 / 深色模式 */
        .sk-logo-bg { fill: url(#skDarkBg); }
        .sk-logo-rim { stroke: url(#skDarkRim); }
        .sk-logo-log { fill: url(#skDarkLog); }
        .sk-logo-beam { fill: url(#skDarkBeam); }
        .sk-logo-stream { fill: url(#skDarkStream); }
        .sk-logo-node { fill: #FFFFFF; }
        .sk-logo-glow { opacity: 0.16; }

        /* 浅色模式自适应切换 */
        html[data-theme="light"] .sk-logo-bg { fill: url(#skLightBg); }
        html[data-theme="light"] .sk-logo-rim { stroke: url(#skLightRim); }
        html[data-theme="light"] .sk-logo-log { fill: url(#skLightLog); }
        html[data-theme="light"] .sk-logo-beam { fill: url(#skLightBeam); }
        html[data-theme="light"] .sk-logo-stream { fill: url(#skLightStream); }
        html[data-theme="light"] .sk-logo-node { fill: #F8FAFC; }
        html[data-theme="light"] .sk-logo-glow { opacity: 0.04; }
      `}</style>

      {/* ── Squircle 容器底座 ── */}
      <rect
        x="47"
        y="47"
        width="418"
        height="418"
        rx="94"
        className="sk-logo-bg"
      />
      <rect
        x="47"
        y="47"
        width="418"
        height="418"
        rx="94"
        strokeWidth="3"
        className="sk-logo-rim"
      />

      {/* ── 能量氛围辉光 ── */}
      <circle
        cx="280"
        cy="256"
        r="110"
        fill="#6366F1"
        className="sk-logo-glow"
      />

      {/* ── 核心图形：Hyper-Stream K ── */}
      <g>
        {/* 纵向 Commit Log 消息队列分区块 */}
        <rect x="145" y="130" width="32" height="60" rx="8" className="sk-logo-log" />
        <rect x="145" y="205" width="32" height="102" rx="8" className="sk-logo-log" />
        <rect x="145" y="322" width="32" height="60" rx="8" className="sk-logo-log" />

        {/* 数据流指示节点 */}
        <circle cx="161" cy="160" r="5" className="sk-logo-node" />
        <circle cx="161" cy="256" r="5" className="sk-logo-node" />
        <circle cx="161" cy="352" r="5" className="sk-logo-node" />

        {/* 生产者写入喷流 (Producer Jet) */}
        <path
          d="M 200 236 L 279 162 C 289 152 306 152 317 162 L 363 208 C 371 216 365 230 354 230 L 290 230 C 282 230 274 234 269 240 L 200 311 Z"
          className="sk-logo-beam"
        />

        {/* 消费者拉取管道 (Consumer Warp) */}
        <path
          d="M 247 266 L 349 364 C 358 373 373 367 373 354 L 373 310 C 373 303 369 296 364 291 L 299 230 C 292 223 281 225 275 233 L 247 266 Z"
          className="sk-logo-stream"
        />

        {/* 核心枢纽节点脉冲光球 */}
        <circle cx="225" cy="256" r="13" className="sk-logo-node" />
        <circle cx="225" cy="256" r="23" fill="#38BDF8" opacity="0.35" />
      </g>
    </svg>
  );
};

export default AppLogo;
