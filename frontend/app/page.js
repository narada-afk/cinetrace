'use client'

import { useState } from 'react'

const ACTORS = ['Allu Arjun', 'Vijay', 'Prabhas', 'Mahesh Babu']

const API_URL = 'http://localhost:8000/compare'

export default function Home() {
  const [actor1, setActor1] = useState(ACTORS[0])
  const [actor2, setActor2] = useState(ACTORS[1])
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  async function handleCompare() {
    setLoading(true)
    setError(null)
    setResult(null)

    try {
      const url = `${API_URL}?actor1=${encodeURIComponent(actor1)}&actor2=${encodeURIComponent(actor2)}`
      const res = await fetch(url)

      if (!res.ok) {
        const data = await res.json()
        throw new Error(data.detail || 'Something went wrong')
      }

      const data = await res.json()
      setResult(data)
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <main style={styles.main}>
      <h1 style={styles.title}>South Cinema Analytics</h1>
      <p style={styles.subtitle}>Compare two actors head to head</p>

      {/* Dropdowns */}
      <div style={styles.controls}>
        <div style={styles.selectGroup}>
          <label style={styles.label}>Actor 1</label>
          <select style={styles.select} value={actor1} onChange={e => setActor1(e.target.value)}>
            {ACTORS.map(name => (
              <option key={name} value={name}>{name}</option>
            ))}
          </select>
        </div>

        <span style={styles.vs}>VS</span>

        <div style={styles.selectGroup}>
          <label style={styles.label}>Actor 2</label>
          <select style={styles.select} value={actor2} onChange={e => setActor2(e.target.value)}>
            {ACTORS.map(name => (
              <option key={name} value={name}>{name}</option>
            ))}
          </select>
        </div>
      </div>

      <button style={styles.button} onClick={handleCompare} disabled={loading}>
        {loading ? 'Comparing...' : 'Compare'}
      </button>

      {/* Error */}
      {error && <p style={styles.error}>{error}</p>}

      {/* Results Table */}
      {result && (
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th}>Metric</th>
              <th style={styles.th}>{result.actor1.name}</th>
              <th style={styles.th}>{result.actor2.name}</th>
            </tr>
          </thead>
          <tbody>
            <tr style={styles.tr}>
              <td style={styles.td}>Total Movies</td>
              <td style={styles.tdValue}>{result.actor1.total_movies}</td>
              <td style={styles.tdValue}>{result.actor2.total_movies}</td>
            </tr>
            <tr style={styles.trAlt}>
              <td style={styles.td}>Avg Rating</td>
              <td style={styles.tdValue}>{result.actor1.avg_rating}</td>
              <td style={styles.tdValue}>{result.actor2.avg_rating}</td>
            </tr>
            <tr style={styles.tr}>
              <td style={styles.td}>Movies After 2015</td>
              <td style={styles.tdValue}>{result.actor1.movies_after_2015}</td>
              <td style={styles.tdValue}>{result.actor2.movies_after_2015}</td>
            </tr>
            <tr style={styles.trAlt}>
              <td style={styles.td}>Avg Box Office (₹ Cr)</td>
              <td style={styles.tdValue}>{result.actor1.avg_box_office}</td>
              <td style={styles.tdValue}>{result.actor2.avg_box_office}</td>
            </tr>
          </tbody>
        </table>
      )}
    </main>
  )
}

const styles = {
  main: {
    maxWidth: 700,
    margin: '60px auto',
    padding: '0 20px',
  },
  title: {
    fontSize: 32,
    fontWeight: 'bold',
    textAlign: 'center',
    marginBottom: 8,
    color: '#1a1a1a',
  },
  subtitle: {
    textAlign: 'center',
    color: '#666',
    marginBottom: 40,
  },
  controls: {
    display: 'flex',
    alignItems: 'flex-end',
    justifyContent: 'center',
    gap: 24,
    marginBottom: 24,
  },
  selectGroup: {
    display: 'flex',
    flexDirection: 'column',
    gap: 6,
  },
  label: {
    fontSize: 14,
    fontWeight: '600',
    color: '#444',
  },
  select: {
    padding: '10px 14px',
    fontSize: 16,
    borderRadius: 8,
    border: '1px solid #ccc',
    backgroundColor: '#fff',
    cursor: 'pointer',
    minWidth: 160,
  },
  vs: {
    fontSize: 18,
    fontWeight: 'bold',
    color: '#888',
    paddingBottom: 10,
  },
  button: {
    display: 'block',
    margin: '0 auto 32px',
    padding: '12px 40px',
    fontSize: 16,
    fontWeight: '600',
    backgroundColor: '#e63946',
    color: '#fff',
    border: 'none',
    borderRadius: 8,
    cursor: 'pointer',
  },
  error: {
    color: 'red',
    textAlign: 'center',
    marginBottom: 16,
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    backgroundColor: '#fff',
    borderRadius: 10,
    overflow: 'hidden',
    boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
  },
  th: {
    padding: '14px 20px',
    textAlign: 'left',
    backgroundColor: '#1a1a2e',
    color: '#fff',
    fontSize: 15,
    fontWeight: '600',
  },
  tr: {
    backgroundColor: '#fff',
  },
  trAlt: {
    backgroundColor: '#f9f9f9',
  },
  td: {
    padding: '14px 20px',
    color: '#444',
    fontSize: 15,
    borderBottom: '1px solid #eee',
  },
  tdValue: {
    padding: '14px 20px',
    fontWeight: '600',
    color: '#1a1a1a',
    fontSize: 15,
    borderBottom: '1px solid #eee',
  },
}
