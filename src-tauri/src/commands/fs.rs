use std::fs;

#[tauri::command]
pub async fn write_text_file(path: String, contents: String) -> Result<(), String> {
    fs::write(&path, contents).map_err(|e| format!("写入文件失败: {e}"))
}
