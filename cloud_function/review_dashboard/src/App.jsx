import { useState, useEffect, useCallback } from "react";

const SAMPLE_DATA = {
  paper_id: "2016_neet_solutions_phase_1_code_a_p_w",
  paper_name: "NEET 2016 Phase 1 Code A",
  questions: [
    {
      q_num: 144, section: "Chemistry", status: "flagged",
      question_text: "In which of the following molecules, all atoms are coplanar?",
      correct_answer: "3",
      options: {
        1: "[DIAGRAM of CH₃-C≡C-CH₃]",
        2: "[DIAGRAM of Biphenyl]",
        3: "[DIAGRAM of Styrene]",
        4: "[DIAGRAM of Cyclohexane derivative]"
      },
      images: [
        { id: "img70", file: "image70.png", current_zone: "option_1", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Biphenyl%0A%E2%AC%A1-%E2%AC%A1" },
        { id: "img71", file: "image71.png", current_zone: "option_1", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=CH₃C%3DC%0ACN₂" },
        { id: "img72", file: "image72.png", current_zone: "option_2", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Biphenyl%0A⬡-⬡" },
        { id: "img73", file: "image73.png", current_zone: "option_4", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Cyclohexane%0A⬡" },
        { id: "img74", file: "image74.png", current_zone: "solution", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Anthracene%0AH-struct" },
      ],
      pdf_page: 37
    },
    {
      q_num: 145, section: "Chemistry", status: "flagged",
      question_text: "Which one of the following structures represents nylon 6,6 polymer?",
      correct_answer: "2",
      options: { 1: "[DIAGRAM of polymer fragment]", 2: "[DIAGRAM of Nylon 6,6]", 3: "[DIAGRAM]", 4: "[DIAGRAM]" },
      images: [
        { id: "img75", file: "image75.png", current_zone: "question", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Nylon%0Astruct" },
        { id: "img77", file: "image77.png", current_zone: "option_1", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Polymer%0Afrag1" },
        { id: "img78", file: "image78.png", current_zone: "option_1", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Polymer%0Afrag2" },
        { id: "img79", file: "image79.png", current_zone: "solution", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Nylon%0Asynthesis" },
      ],
      pdf_page: 37
    },
    {
      q_num: 148, section: "Chemistry", status: "flagged",
      question_text: "Which one of the following nitro-compounds does not react with nitrous acid?",
      correct_answer: "1",
      options: { 1: "CH₃-CH₂-CH₂-NO₂ [DIAGRAM]", 2: "CH₃-CH(NO₂)-CH₃ [DIAGRAM]", 3: "(CH₃)₃C-NO₂ [DIAGRAM]", 4: "(CH₃)₂CHCH₂NO₂ [DIAGRAM]" },
      images: [
        { id: "img86", file: "image86.png", current_zone: "question", url: "https://placehold.co/120x90/4a1a1a/e0e0e0?text=JUNK%0A□□□□" },
        { id: "img87", file: "image87.png", current_zone: "question", url: "https://placehold.co/120x90/4a1a1a/e0e0e0?text=JUNK%0A□□□□" },
        { id: "img88", file: "image88.png", current_zone: "question", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=(CH₃)₃C%0ANO₂" },
        { id: "img89", file: "image89.png", current_zone: "question", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=CH₃CH%0ANO₂" },
        { id: "img90", file: "image90.png", current_zone: "option_3", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=H₃C-C-NO₂%0Atertiary" },
        { id: "img91", file: "image91.png", current_zone: "option_4", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=H₃C-CH%0ACH₂NO₂" },
      ],
      pdf_page: 39
    },
    {
      q_num: 151, section: "Chemistry", status: "flagged",
      question_text: "In the given reaction: [DIAGRAM] The product P is:",
      correct_answer: "1",
      options: { 1: "[DIAGRAM of Phenylcyclohexane]", 2: "[DIAGRAM of Tetralin derivative]", 3: "[DIAGRAM of Indane derivative]", 4: "[DIAGRAM]" },
      images: [
        { id: "img95", file: "image95.png", current_zone: "option_1", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Phenyl%0Acyclohex" },
        { id: "img96", file: "image96.png", current_zone: "option_2", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Tetralin%0Aderiv" },
        { id: "img97", file: "image97.png", current_zone: "option_2", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Tricyclic%0Asystem" },
        { id: "img98", file: "image98.png", current_zone: "option_2", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Fluoro%0Abiphenyl" },
        { id: "img99", file: "image99.png", current_zone: "solution", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Friedel%0ACrafts" },
      ],
      pdf_page: 41
    },
    {
      q_num: 152, section: "Chemistry", status: "approved",
      question_text: "A given nitrogen-containing aromatic compound A reacts with Sn/HCl...",
      correct_answer: "4",
      options: { 1: "Benzonitrile [DIAGRAM]", 2: "Benzamide [DIAGRAM]", 3: "Aniline [DIAGRAM]", 4: "Nitrobenzene [DIAGRAM]" },
      images: [
        { id: "img100", file: "image100.png", current_zone: "option_1", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=C₆H₅-CN" },
        { id: "img101", file: "image101.png", current_zone: "option_2", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=C₆H₅-CONH₂" },
        { id: "img102", file: "image102.png", current_zone: "option_3", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=C₆H₅-NH₂" },
        { id: "img103", file: "image103.png", current_zone: "option_4", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=C₆H₅-NO₂" },
      ],
      pdf_page: 41
    },
    {
      q_num: 154, section: "Chemistry", status: "flagged",
      question_text: "The correct structure of the product A formed in the reaction: [DIAGRAM]",
      correct_answer: "1",
      options: { 1: "[DIAGRAM of 1-phenylethanol]", 2: "[DIAGRAM of cyclohexylmethanol]", 3: "[DIAGRAM of tertiary alcohol]", 4: "[DIAGRAM of cyclohexanone]" },
      images: [
        { id: "img105", file: "image105.png", current_zone: "question", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=DMF%0AH-C-N" },
        { id: "img106", file: "image106.png", current_zone: "question", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=Cyclohex%0A%3DO" },
        { id: "img107", file: "image107.png", current_zone: "question", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=OH%0Acyclohex" },
        { id: "img108", file: "image108.png", current_zone: "question", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=OH%0Aphenyl" },
        { id: "img109", file: "image109.png", current_zone: "question", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=OH%0Atertiary" },
        { id: "img110", file: "image110.png", current_zone: "question", url: "https://placehold.co/120x90/1a1a2e/e0e0e0?text=O%3D%0Acyclohex" },
      ],
      pdf_page: 42
    },
  ]
};

const ZONES = [
  { id: "question", label: "Question", color: "#2563eb", bg: "#eff6ff" },
  { id: "option_1", label: "Option 1", color: "#7c3aed", bg: "#f5f3ff" },
  { id: "option_2", label: "Option 2", color: "#7c3aed", bg: "#f5f3ff" },
  { id: "option_3", label: "Option 3", color: "#7c3aed", bg: "#f5f3ff" },
  { id: "option_4", label: "Option 4", color: "#7c3aed", bg: "#f5f3ff" },
  { id: "solution", label: "Solution", color: "#059669", bg: "#ecfdf5" },
  { id: "junk", label: "Junk ✕", color: "#dc2626", bg: "#fef2f2" },
];

const StatusBadge = ({ status }) => {
  const styles = {
    flagged: { background: "#fef3c7", color: "#92400e", border: "1px solid #f59e0b" },
    approved: { background: "#d1fae5", color: "#065f46", border: "1px solid #10b981" },
    modified: { background: "#e0e7ff", color: "#3730a3", border: "1px solid #6366f1" },
  };
  return (
    <span style={{
      ...styles[status],
      padding: "2px 10px",
      borderRadius: "99px",
      fontSize: "11px",
      fontWeight: 600,
      letterSpacing: "0.03em",
      textTransform: "uppercase",
    }}>
      {status}
    </span>
  );
};

const ImageCard = ({ image, onZoneChange, isSelected, onSelect }) => {
  const zone = ZONES.find(z => z.id === image.current_zone) || ZONES[0];
  return (
    <div
      onClick={() => onSelect(image.id)}
      style={{
        border: isSelected ? "2px solid #6366f1" : "1px solid #e2e8f0",
        borderRadius: "10px",
        padding: "8px",
        background: isSelected ? "#f0f0ff" : "#fff",
        cursor: "pointer",
        transition: "all 0.15s ease",
        boxShadow: isSelected ? "0 0 0 3px rgba(99,102,241,0.15)" : "0 1px 2px rgba(0,0,0,0.04)",
      }}
    >
      <img
        src={image.url}
        alt={image.file}
        style={{
          width: "100%",
          height: "80px",
          objectFit: "contain",
          borderRadius: "6px",
          background: "#f8fafc",
          display: "block",
        }}
      />
      <div style={{ marginTop: "6px", fontSize: "10px", color: "#64748b", fontFamily: "monospace" }}>
        {image.file}
      </div>
      <select
        value={image.current_zone}
        onChange={(e) => onZoneChange(image.id, e.target.value)}
        onClick={(e) => e.stopPropagation()}
        style={{
          width: "100%",
          marginTop: "4px",
          padding: "4px 6px",
          fontSize: "11px",
          fontWeight: 600,
          border: `1.5px solid ${zone.color}`,
          borderRadius: "6px",
          background: zone.bg,
          color: zone.color,
          cursor: "pointer",
          outline: "none",
        }}
      >
        {ZONES.map(z => (
          <option key={z.id} value={z.id}>{z.label}</option>
        ))}
      </select>
    </div>
  );
};

const QuestionPanel = ({ question, onZoneChange, onApprove, changes }) => {
  const [selectedImg, setSelectedImg] = useState(null);
  const hasChanges = changes && Object.keys(changes).length > 0;
  const correctOpt = question.correct_answer;

  return (
    <div style={{
      background: "#fff",
      borderRadius: "14px",
      border: "1px solid #e2e8f0",
      overflow: "hidden",
    }}>
      {/* Header */}
      <div style={{
        padding: "16px 20px",
        background: "linear-gradient(135deg, #0f172a 0%, #1e293b 100%)",
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
          <span style={{
            fontSize: "22px",
            fontWeight: 800,
            color: "#fff",
            fontFamily: "'JetBrains Mono', monospace",
          }}>
            Q{question.q_num}
          </span>
          <span style={{
            fontSize: "12px",
            color: "#94a3b8",
            padding: "2px 8px",
            background: "rgba(255,255,255,0.08)",
            borderRadius: "6px",
          }}>
            {question.section}
          </span>
          <StatusBadge status={hasChanges ? "modified" : question.status} />
        </div>
        <div style={{ display: "flex", gap: "8px" }}>
          <span style={{
            fontSize: "11px",
            color: "#94a3b8",
            padding: "4px 10px",
            background: "rgba(255,255,255,0.06)",
            borderRadius: "6px",
          }}>
            Page {question.pdf_page}
          </span>
          <span style={{
            fontSize: "11px",
            color: "#10b981",
            padding: "4px 10px",
            background: "rgba(16,185,129,0.1)",
            borderRadius: "6px",
            fontWeight: 600,
          }}>
            Answer: ({correctOpt})
          </span>
        </div>
      </div>

      {/* Question text */}
      <div style={{
        padding: "14px 20px",
        fontSize: "13px",
        color: "#334155",
        lineHeight: 1.6,
        borderBottom: "1px solid #f1f5f9",
        background: "#fafbfc",
      }}>
        {question.question_text}
      </div>

      {/* Options */}
      <div style={{ padding: "12px 20px", borderBottom: "1px solid #f1f5f9" }}>
        {Object.entries(question.options).map(([num, text]) => {
          const isCorrect = num === correctOpt;
          const zoneImages = question.images.filter(
            img => (changes?.[img.id] || img.current_zone) === `option_${num}`
          );
          return (
            <div key={num} style={{
              display: "flex",
              alignItems: "flex-start",
              gap: "10px",
              padding: "8px 10px",
              marginBottom: "4px",
              borderRadius: "8px",
              background: isCorrect ? "#f0fdf4" : "transparent",
              border: isCorrect ? "1px solid #bbf7d0" : "1px solid transparent",
            }}>
              <span style={{
                fontWeight: 700,
                fontSize: "12px",
                color: isCorrect ? "#15803d" : "#64748b",
                minWidth: "24px",
                height: "24px",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                borderRadius: "6px",
                background: isCorrect ? "#dcfce7" : "#f1f5f9",
                flexShrink: 0,
              }}>
                {num}
              </span>
              <div style={{ flex: 1, fontSize: "12px", color: "#475569", lineHeight: 1.5 }}>
                {text}
                {isCorrect && <span style={{ color: "#16a34a", fontWeight: 600, marginLeft: "6px" }}>✓</span>}
              </div>
              <div style={{ display: "flex", gap: "4px", flexShrink: 0 }}>
                {zoneImages.map(img => (
                  <img key={img.id} src={img.url} alt="" style={{
                    width: "40px", height: "32px", objectFit: "contain",
                    borderRadius: "4px", border: "1px solid #e2e8f0", background: "#f8fafc"
                  }} />
                ))}
                {zoneImages.length === 0 && (
                  <span style={{ fontSize: "10px", color: "#cbd5e1", fontStyle: "italic" }}>no image</span>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Image assignment grid */}
      <div style={{ padding: "16px 20px" }}>
        <div style={{
          fontSize: "11px",
          fontWeight: 700,
          color: "#94a3b8",
          textTransform: "uppercase",
          letterSpacing: "0.08em",
          marginBottom: "10px",
        }}>
          Extracted images — click dropdown to reassign zone
        </div>
        <div style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))",
          gap: "8px",
        }}>
          {question.images.map(img => (
            <ImageCard
              key={img.id}
              image={{ ...img, current_zone: changes?.[img.id] || img.current_zone }}
              onZoneChange={onZoneChange}
              isSelected={selectedImg === img.id}
              onSelect={setSelectedImg}
            />
          ))}
        </div>
      </div>

      {/* Actions */}
      <div style={{
        padding: "12px 20px",
        borderTop: "1px solid #f1f5f9",
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        background: "#fafbfc",
      }}>
        <div style={{ fontSize: "11px", color: "#94a3b8" }}>
          {hasChanges
            ? `${Object.keys(changes).length} change(s) pending`
            : question.status === "approved" ? "Approved ✓" : "No changes yet"
          }
        </div>
        <button
          onClick={onApprove}
          style={{
            padding: "8px 24px",
            fontSize: "12px",
            fontWeight: 700,
            color: "#fff",
            background: hasChanges
              ? "linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)"
              : "linear-gradient(135deg, #10b981 0%, #059669 100%)",
            border: "none",
            borderRadius: "8px",
            cursor: "pointer",
            letterSpacing: "0.02em",
            boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
          }}
        >
          {hasChanges ? "Save & Approve →" : question.status === "approved" ? "Approved ✓" : "Approve →"}
        </button>
      </div>
    </div>
  );
};

export default function ReviewDashboard() {
  const [data] = useState(SAMPLE_DATA);
  const [currentIdx, setCurrentIdx] = useState(0);
  const [filter, setFilter] = useState("all");
  const [allChanges, setAllChanges] = useState({});
  const [statuses, setStatuses] = useState(() => {
    const s = {};
    data.questions.forEach(q => s[q.q_num] = q.status);
    return s;
  });

  const filtered = data.questions.filter(q =>
    filter === "all" || (filter === "flagged" && statuses[q.q_num] === "flagged") ||
    (filter === "approved" && statuses[q.q_num] === "approved")
  );

  const currentQ = filtered[currentIdx] || filtered[0];

  const handleZoneChange = useCallback((imgId, newZone) => {
    setAllChanges(prev => ({
      ...prev,
      [currentQ.q_num]: {
        ...(prev[currentQ.q_num] || {}),
        [imgId]: newZone,
      }
    }));
  }, [currentQ]);

  const handleApprove = useCallback(() => {
    setStatuses(prev => ({ ...prev, [currentQ.q_num]: "approved" }));
    if (currentIdx < filtered.length - 1) {
      setCurrentIdx(currentIdx + 1);
    }
  }, [currentQ, currentIdx, filtered.length]);

  useEffect(() => {
    const handler = (e) => {
      if (e.key === "ArrowRight" && currentIdx < filtered.length - 1) setCurrentIdx(currentIdx + 1);
      if (e.key === "ArrowLeft" && currentIdx > 0) setCurrentIdx(currentIdx - 1);
      if (e.key === "Enter" && e.metaKey) handleApprove();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [currentIdx, filtered.length, handleApprove]);

  const flaggedCount = data.questions.filter(q => statuses[q.q_num] === "flagged").length;
  const approvedCount = data.questions.filter(q => statuses[q.q_num] === "approved").length;

  return (
    <div style={{
      fontFamily: "'Inter', -apple-system, sans-serif",
      background: "#f8fafc",
      minHeight: "100vh",
      color: "#0f172a",
    }}>
      <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet" />

      {/* Top bar */}
      <div style={{
        padding: "12px 24px",
        background: "#0f172a",
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        borderBottom: "1px solid #1e293b",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "16px" }}>
          <span style={{
            fontSize: "15px",
            fontWeight: 800,
            color: "#f1f5f9",
            letterSpacing: "-0.02em",
          }}>
            NEET Diagram Review
          </span>
          <span style={{
            fontSize: "12px",
            color: "#64748b",
            padding: "3px 10px",
            background: "#1e293b",
            borderRadius: "6px",
          }}>
            {data.paper_name}
          </span>
        </div>
        <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
          <div style={{
            display: "flex",
            gap: "2px",
            background: "#1e293b",
            borderRadius: "8px",
            padding: "2px",
          }}>
            {["all", "flagged", "approved"].map(f => (
              <button key={f} onClick={() => { setFilter(f); setCurrentIdx(0); }} style={{
                padding: "5px 14px",
                fontSize: "11px",
                fontWeight: 600,
                border: "none",
                borderRadius: "6px",
                cursor: "pointer",
                background: filter === f ? "#334155" : "transparent",
                color: filter === f ? "#f1f5f9" : "#64748b",
                textTransform: "capitalize",
              }}>
                {f} {f === "flagged" ? `(${flaggedCount})` : f === "approved" ? `(${approvedCount})` : `(${data.questions.length})`}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Main layout */}
      <div style={{ display: "flex", height: "calc(100vh - 52px)" }}>

        {/* Left sidebar — question list */}
        <div style={{
          width: "220px",
          borderRight: "1px solid #e2e8f0",
          background: "#fff",
          overflowY: "auto",
          flexShrink: 0,
        }}>
          <div style={{
            padding: "12px 14px 8px",
            fontSize: "10px",
            fontWeight: 700,
            color: "#94a3b8",
            textTransform: "uppercase",
            letterSpacing: "0.08em",
          }}>
            Questions ({filtered.length})
          </div>
          {filtered.map((q, idx) => {
            const isActive = idx === currentIdx;
            const st = statuses[q.q_num];
            const hasChange = allChanges[q.q_num] && Object.keys(allChanges[q.q_num]).length > 0;
            return (
              <div
                key={q.q_num}
                onClick={() => setCurrentIdx(idx)}
                style={{
                  padding: "10px 14px",
                  cursor: "pointer",
                  background: isActive ? "#f0f4ff" : "transparent",
                  borderLeft: isActive ? "3px solid #4f46e5" : "3px solid transparent",
                  borderBottom: "1px solid #f8fafc",
                  transition: "all 0.1s ease",
                }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{
                    fontSize: "13px",
                    fontWeight: isActive ? 700 : 500,
                    color: isActive ? "#1e293b" : "#475569",
                    fontFamily: "'JetBrains Mono', monospace",
                  }}>
                    Q{q.q_num}
                  </span>
                  <StatusBadge status={hasChange ? "modified" : st} />
                </div>
                <div style={{
                  fontSize: "10px",
                  color: "#94a3b8",
                  marginTop: "3px",
                  display: "flex",
                  gap: "8px",
                }}>
                  <span>{q.images.length} imgs</span>
                  <span>{q.section}</span>
                </div>
              </div>
            );
          })}
        </div>

        {/* Right content — split view */}
        <div style={{ flex: 1, display: "flex", overflow: "hidden" }}>

          {/* PDF reference panel */}
          <div style={{
            width: "45%",
            borderRight: "1px solid #e2e8f0",
            display: "flex",
            flexDirection: "column",
            background: "#f1f5f9",
          }}>
            <div style={{
              padding: "10px 16px",
              background: "#fff",
              borderBottom: "1px solid #e2e8f0",
              fontSize: "11px",
              fontWeight: 700,
              color: "#64748b",
              textTransform: "uppercase",
              letterSpacing: "0.06em",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}>
              <span>Original PDF — Page {currentQ?.pdf_page}</span>
              <span style={{ color: "#94a3b8", fontWeight: 400, textTransform: "none" }}>
                Use this as reference
              </span>
            </div>
            <div style={{
              flex: 1,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: "24px",
            }}>
              <div style={{
                width: "100%",
                maxWidth: "400px",
                background: "#fff",
                borderRadius: "12px",
                border: "1px solid #e2e8f0",
                padding: "32px",
                textAlign: "center",
                boxShadow: "0 4px 12px rgba(0,0,0,0.04)",
              }}>
                <div style={{
                  width: "64px",
                  height: "64px",
                  margin: "0 auto 16px",
                  background: "#f1f5f9",
                  borderRadius: "12px",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontSize: "28px",
                  color: "#94a3b8",
                }}>
                  📄
                </div>
                <div style={{ fontSize: "14px", fontWeight: 600, color: "#334155", marginBottom: "8px" }}>
                  PDF Viewer
                </div>
                <div style={{ fontSize: "12px", color: "#94a3b8", lineHeight: 1.6 }}>
                  In the full version, this panel shows the actual PDF page for Q{currentQ?.q_num} (page {currentQ?.pdf_page}).
                  You compare the original layout with the extracted images on the right.
                </div>
                <div style={{
                  marginTop: "16px",
                  padding: "8px 16px",
                  background: "#f8fafc",
                  borderRadius: "8px",
                  fontSize: "11px",
                  color: "#64748b",
                  fontFamily: "'JetBrains Mono', monospace",
                }}>
                  gs://...input-papers/{data.paper_id}.pdf#page={currentQ?.pdf_page}
                </div>
              </div>
            </div>
          </div>

          {/* Review panel */}
          <div style={{
            width: "55%",
            overflowY: "auto",
            padding: "16px",
          }}>
            {currentQ && (
              <>
                <QuestionPanel
                  question={currentQ}
                  onZoneChange={handleZoneChange}
                  onApprove={handleApprove}
                  changes={allChanges[currentQ.q_num]}
                />

                {/* Navigation */}
                <div style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  marginTop: "12px",
                  padding: "0 4px",
                }}>
                  <button
                    disabled={currentIdx === 0}
                    onClick={() => setCurrentIdx(currentIdx - 1)}
                    style={{
                      padding: "6px 16px",
                      fontSize: "12px",
                      fontWeight: 600,
                      border: "1px solid #e2e8f0",
                      borderRadius: "8px",
                      background: "#fff",
                      color: currentIdx === 0 ? "#cbd5e1" : "#475569",
                      cursor: currentIdx === 0 ? "default" : "pointer",
                    }}
                  >
                    ← Prev
                  </button>
                  <span style={{ fontSize: "11px", color: "#94a3b8" }}>
                    {currentIdx + 1} / {filtered.length} · Arrow keys to navigate · ⌘Enter to approve
                  </span>
                  <button
                    disabled={currentIdx >= filtered.length - 1}
                    onClick={() => setCurrentIdx(currentIdx + 1)}
                    style={{
                      padding: "6px 16px",
                      fontSize: "12px",
                      fontWeight: 600,
                      border: "1px solid #e2e8f0",
                      borderRadius: "8px",
                      background: "#fff",
                      color: currentIdx >= filtered.length - 1 ? "#cbd5e1" : "#475569",
                      cursor: currentIdx >= filtered.length - 1 ? "default" : "pointer",
                    }}
                  >
                    Next →
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}