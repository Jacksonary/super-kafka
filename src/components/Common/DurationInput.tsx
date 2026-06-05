import { useEffect, useRef, useState } from "react";
import { InputNumber, Select, Space, Switch } from "antd";
import { DURATION_UNIT_MS, type DurationUnit } from "../../utils/format";

const UNIT_OPTIONS: { value: DurationUnit; label: string }[] = [
  { value: "d", label: "Days" },
  { value: "h", label: "Hours" },
  { value: "m", label: "Minutes" },
  { value: "s", label: "Seconds" },
  { value: "ms", label: "Milliseconds" },
];

export interface DurationInputProps {
  /**
   * Current value in milliseconds. null/undefined = empty (caller should skip submit). -1 = Forever.
   */
  value?: number | null;
  /** Emits null when the field is empty / forever-toggled-then-emptied. */
  onChange?: (ms: number | null) => void;
  defaultUnit?: DurationUnit;
  defaultAmount?: number;
  disabled?: boolean;
}

function pickUnit(ms: number): { unit: DurationUnit; amount: number } {
  if (ms <= 0) return { unit: "h", amount: ms };
  for (const u of ["d", "h", "m", "s"] as DurationUnit[]) {
    const f = DURATION_UNIT_MS[u];
    if (ms % f === 0) return { unit: u, amount: ms / f };
  }
  return { unit: "ms", amount: ms };
}

export default function DurationInput({
  value,
  onChange,
  defaultUnit = "h",
  defaultAmount,
  disabled,
}: DurationInputProps) {
  const initial = (() => {
    if (value == null) return { unit: defaultUnit, amount: defaultAmount ?? null, forever: false };
    if (value === -1) return { unit: defaultUnit, amount: defaultAmount ?? null, forever: true };
    const p = pickUnit(value);
    return { unit: p.unit, amount: p.amount, forever: false };
  })();

  const [unit, setUnit] = useState<DurationUnit>(initial.unit);
  const [amount, setAmount] = useState<number | null>(initial.amount);
  const [forever, setForever] = useState<boolean>(initial.forever);

  // Track our own emits so we can ignore the controlled-value round-trip
  // (parent passes back the same ms we just emitted -> would otherwise wipe
  // the user's unit choice via pickUnit).
  const lastEmittedRef = useRef<number | null | undefined>(undefined);
  // Track whether the user has touched any control. Before that, we must not
  // emit (so leaving the field blank is preserved as "not filled").
  const touched = useRef(false);

  // Sync external value changes (modal reopen / form reset). Skip if the
  // incoming value is exactly what we just emitted.
  useEffect(() => {
    if (value === undefined) return; // uncontrolled
    if (value === lastEmittedRef.current) return; // round-trip from our own emit
    if (value === null) {
      setForever(false);
      setAmount(null);
      return;
    }
    if (value === -1) {
      setForever(true);
      return;
    }
    setForever(false);
    const p = pickUnit(value);
    setUnit(p.unit);
    setAmount(p.amount);
  }, [value]);

  // Emit upward only AFTER first user interaction.
  useEffect(() => {
    if (!touched.current) return;
    let next: number | null;
    if (forever) next = -1;
    else if (amount == null) next = null;
    else next = amount * DURATION_UNIT_MS[unit];
    lastEmittedRef.current = next;
    onChange?.(next);
  }, [unit, amount, forever]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <Space style={{ width: "100%" }}>
      <Space.Compact>
        <InputNumber
          min={0}
          step={1}
          precision={0}
          style={{ width: 120 }}
          value={amount}
          onChange={(v) => {
            touched.current = true;
            if (v == null) {
              setAmount(null);
              return;
            }
            if (typeof v === "number" && Number.isFinite(v) && v >= 0) {
              setAmount(Math.floor(v));
            }
          }}
          disabled={disabled || forever}
        />
        <Select
          style={{ width: 120 }}
          value={unit}
          onChange={(u) => {
            touched.current = true;
            setUnit(u);
          }}
          options={UNIT_OPTIONS}
          disabled={disabled || forever}
        />
      </Space.Compact>
      <Space size={6}>
        <Switch
          size="small"
          checked={forever}
          onChange={(v) => {
            touched.current = true;
            setForever(v);
          }}
          disabled={disabled}
        />
        <span>Forever</span>
      </Space>
    </Space>
  );
}
