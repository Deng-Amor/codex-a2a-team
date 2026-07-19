<script setup>
import { onMounted, ref } from 'vue'
const agents=ref([]),error=ref('')
async function load(){try{agents.value=await fetch((import.meta.env.VITE_API_URL||'http://127.0.0.1:8010')+'/api/agents').then(r=>r.json())}catch(e){error.value=e.message}}
onMounted(load)
</script>
<template><main><header><small>A2A CONTROL PLANE</small><h1>Agent Office</h1><p>节点由 PostgreSQL 动态提供</p></header><p v-if="error">{{ error }}</p><section><article v-for="agent in agents" :key="agent.key"><b>{{ agent.name }}</b><span>{{ agent.key }}</span><small>{{ agent.role }} · {{ agent.capabilities || '未声明能力' }}</small></article></section></main></template>
<style scoped>main{max-width:900px;margin:auto;padding:32px;font-family:system-ui;color:#182135}header{padding:28px;border-radius:20px;background:#13205d;color:#fff}header p{color:#ccd6ff}section{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-top:20px}article{display:grid;gap:7px;padding:18px;border:1px solid #dce3ed;border-radius:14px;background:#fff}span,small{color:#738096}</style>
